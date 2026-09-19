package com.strangequark.loggerservice;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "logger.kubernetes.enabled", havingValue = "true")
public class KubernetesLogTailer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesLogTailer.class);

    private final OpenSearchService openSearchService;
    private final Map<String, Future<?>> activeTails = new ConcurrentHashMap<>();
    private KubernetesClient kubernetesClient;
    private ThreadPoolExecutor executor;
    private Watch podWatch;

    @Value("${logger.kubernetes.namespace}")
    private String namespace;

    @Value("${logger.tailer.thread-count}")
    private int tailerThreadCount;

    public KubernetesLogTailer(OpenSearchService openSearchService) {
        this.openSearchService = openSearchService;
    }

    @PostConstruct
    public void start() {
        executor = new ThreadPoolExecutor(
                tailerThreadCount,
                tailerThreadCount,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(tailerThreadCount)
        );

        kubernetesClient = new KubernetesClientBuilder().build();
        kubernetesClient.pods().inNamespace(namespace)
                .withLabel("com.msinit.log", "true")
                .list().getItems()
                .forEach(this::startTailing);

        podWatch = kubernetesClient.pods().inNamespace(namespace)
                .withLabel("com.msinit.log", "true")
                .watch(new Watcher<>() {
                    @Override
                    public void eventReceived(Action action, Pod pod) {
                        if(action == Action.ADDED || action == Action.MODIFIED)
                            startTailing(pod);
                    }

                    @Override
                    public void onClose(WatcherException exception) {
                        if(exception != null)
                            LOGGER.error("Kubernetes Pod watch stopped: {}", exception.getMessage());
                    }
                });
    }

    @PreDestroy
    public void stop() {
        if(podWatch != null)
            podWatch.close();

        activeTails.forEach((containerId, future) -> future.cancel(true));

        if(executor != null)
            executor.shutdownNow();

        if(kubernetesClient != null)
            kubernetesClient.close();
    }

    private void startTailing(Pod pod) {
        if(pod.getStatus() == null || !"Running".equals(pod.getStatus().getPhase()))
            return;

        pod.getSpec().getContainers().forEach(container -> {
            String containerId = pod.getMetadata().getUid() + ":" + container.getName();

            if(activeTails.containsKey(containerId))
                return;

            Future<?> future = executor.submit(() -> tailPodLogs(pod, container.getName(), containerId));
            activeTails.put(containerId, future);
        });
    }

    private void tailPodLogs(Pod pod, String containerName, String containerId) {
        String podName = pod.getMetadata().getName();
        String serviceName = pod.getMetadata().getLabels().get("com.msinit.service-name");

        try {
            String existingLogs = kubernetesClient.pods().inNamespace(namespace)
                    .withName(podName)
                    .inContainer(containerName)
                    .usingTimestamps()
                    .getLog();
            processLogs(existingLogs, containerId, serviceName);

            try(LogWatch logWatch = kubernetesClient.pods().inNamespace(namespace)
                    .withName(podName)
                    .inContainer(containerName)
                    .usingTimestamps()
                    .watchLog();
                BufferedReader reader = new BufferedReader(new InputStreamReader(logWatch.getOutput()))) {
                String line;
                while((line = reader.readLine()) != null)
                    processLine(line, containerId, serviceName);
            }
        } catch(Exception ex) {
            LOGGER.error("Stopped tailing Kubernetes Pod {}: {}", podName, ex.getMessage());
        } finally {
            activeTails.remove(containerId);
        }
    }

    void processLogs(String logs, String containerId, String serviceName) {
        if(logs == null || logs.isBlank())
            return;

        for(String line : logs.split("\\R"))
            processLine(line, containerId, serviceName);
    }

    void processLine(String line, String containerId, String serviceName) {
        try {
            int separator = line.indexOf(" ");
            Instant timestamp = Instant.parse(line.substring(0, separator));
            LogEntry entry = new LogEntry();
            entry.setContainerId(containerId);
            entry.setServiceName(serviceName);
            entry.setStream("stdout");
            entry.setMessage(line.substring(separator + 1).trim());
            entry.setTimestamp(timestamp);
            openSearchService.indexLog(entry, getLogId(containerId, line));
        } catch(Exception ex) {
            LOGGER.error("Unable to process Kubernetes log line: {}", ex.getMessage());
        }
    }

    String getLogId(String containerId, String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest((containerId + ":" + line).getBytes());
            return HexFormat.of().formatHex(hash);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}

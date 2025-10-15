package com.strangequark.loggerservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@Component
public class DockerLogTailer {

    private final OpenSearchService openSearchService;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<Path, Future<?>> activeTails = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();

    private static final Path DOCKER_LOG_DIR = Paths.get("/var/lib/docker/containers");

    public DockerLogTailer(OpenSearchService openSearchService) {
        this.openSearchService = openSearchService;
    }

    @PostConstruct
    public void start() {
        new Thread(this::watchContainers, "docker-log-watcher").start();
    }

    private void watchContainers() {
        System.out.println("Watching Docker containers directory: " + DOCKER_LOG_DIR);
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            DOCKER_LOG_DIR.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            // Tail all existing containers on startup
            Files.list(DOCKER_LOG_DIR)
                    .filter(Files::isDirectory)
                    .forEach(this::startTailingIfNeeded);

            // Watch for new containers
            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path newDir = DOCKER_LOG_DIR.resolve((Path) event.context());
                    startTailingIfNeeded(newDir);
                }
                key.reset();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startTailingIfNeeded(Path containerDir) {
        Path logFile = containerDir.resolve(containerDir.getFileName().toString() + "-json.log");
        if (!Files.exists(logFile) || activeTails.containsKey(logFile)) return;

        System.out.println("Starting to tail " + logFile);
        Future<?> future = executor.submit(() -> tailFile(containerDir, logFile));
        activeTails.put(logFile, future);
    }

    private void tailFile(Path containerDir, Path logFile) {
        String containerId = containerDir.getFileName().toString();
        String serviceName = resolveServiceName(containerDir);
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "r")) {
            // Move to end so we only stream new logs
            raf.seek(raf.length());

            String line;
            while (true) {
                line = raf.readLine();
                if (line == null) {
                    Thread.sleep(500); // wait for new data
                    continue;
                }
                processLine(line, containerId, serviceName);
            }
        } catch (Exception e) {
            System.err.println("Stopped tailing " + logFile + ": " + e.getMessage());
        } finally {
            activeTails.remove(logFile);
        }
    }

    private void processLine(String line, String containerId, String serviceName) {
        try {
            JsonNode node = mapper.readTree(line);
            LogEntry entry = new LogEntry();
            entry.setContainerId(containerId);
            entry.setServiceName(serviceName);
            entry.setStream(node.path("stream").asText("stdout"));
            entry.setMessage(node.path("log").asText().trim());
            entry.setTimestamp(Instant.parse(node.path("time").asText()));
            openSearchService.indexLog(entry);
        } catch (Exception ignored) {
        }
    }

    private String resolveServiceName(Path containerDir) {
        try {
            Path configPath = containerDir.resolve("config.v2.json");
            if (Files.exists(configPath)) {
                JsonNode cfg = mapper.readTree(Files.readString(configPath));
                return cfg.path("Name").asText("").replace("/", "");
            }
        } catch (Exception ignored) {}
        return containerDir.getFileName().toString();
    }
}

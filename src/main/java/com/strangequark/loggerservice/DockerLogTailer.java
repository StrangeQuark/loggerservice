package com.strangequark.loggerservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

@Component
public class DockerLogTailer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerLogTailer.class);

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
        new Thread(() -> watchContainers(), "docker-log-watcher").start();
    }

    private void watchContainers() {
        LOGGER.info("Watching Docker containers directory: " + DOCKER_LOG_DIR);
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            DOCKER_LOG_DIR.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            // Tail all existing containers on startup
            try (Stream<Path> containerDirs = Files.list(DOCKER_LOG_DIR)) {
                containerDirs.filter(Files::isDirectory)
                        .filter(this::isLogCollectionEnabled)
                        .forEach(this::startTailingIfNeeded);
            }

            // Watch for new containers
            while (true) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path newDir = DOCKER_LOG_DIR.resolve((Path) event.context());
                    if (Files.isDirectory(newDir))
                        startTailingIfNeeded(newDir);
                }
                key.reset();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startTailingIfNeeded(Path containerDir) {
        LOGGER.info("Container directory detected: " + containerDir);
        Path logFile = containerDir.resolve(containerDir.getFileName().toString() + "-json.log");

        if (activeTails.containsKey(logFile))
            return;

        // Watch the container directory for the log file if not present
        executor.submit(() -> {
            try {
                if (Files.exists(logFile)) {
                    if (isLogCollectionEnabled(containerDir))
                        startTail(containerDir, logFile);
                    else
                        LOGGER.info("Skipping log collection for {}", containerDir);
                    return;
                }

                LOGGER.info("Waiting for log file to appear: {}", logFile);
                try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
                    containerDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

                    while (true) {
                        WatchKey key = watchService.take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            Path created = containerDir.resolve((Path) event.context());
                            if (created.equals(logFile)) {
                                if (isLogCollectionEnabled(containerDir))
                                    startTail(containerDir, logFile);
                                else
                                    LOGGER.info("Skipping log collection for {}", containerDir);
                                return;
                            }
                        }
                        key.reset();
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error watching container dir {}: {}", containerDir, e.getMessage());
            }
        });
    }

    boolean isLogCollectionEnabled(Path containerDir) {
        try {
            Path configPath = containerDir.resolve("config.v2.json");
            if (!Files.exists(configPath))
                return false;

            JsonNode config = mapper.readTree(Files.readString(configPath));
            return config.path("Config")
                    .path("Labels")
                    .path("com.msinit.log")
                    .asBoolean(false);
        } catch (Exception e) {
            LOGGER.error("Error reading container config {}: {}", containerDir, e.getMessage());
            return false;
        }
    }

    private void startTail(Path containerDir, Path logFile) {
        try {
            // Process all existing logs first
            try (Stream<Path> files = Files.list(containerDir)) {
                LOGGER.info("Processing existing logs: {}", logFile);
                files.filter(f -> f.getFileName().toString().startsWith(containerDir.getFileName().toString() + "-json.log"))
                        .sorted(Comparator.comparing(Path::toString))
                        .forEach(f -> processHistoricalLogs(containerDir, f));
            }

            // Then tail the active one
            LOGGER.info("Starting to tail {}", logFile);
            Future<?> future = executor.submit(() -> tailFile(containerDir, logFile));
            activeTails.put(logFile, future);

        } catch (Exception e) {
            LOGGER.error("Failed to start tail for {}: {}", logFile, e.getMessage());
        }
    }

    private void processHistoricalLogs(Path containerDir, Path file) {
        LOGGER.info("Processing historical logs from {}", file);
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            String containerId = containerDir.getFileName().toString();
            String serviceName = resolveServiceName(containerDir);

            while ((line = reader.readLine()) != null) {
                processLine(line, containerId, serviceName);
            }
        } catch (IOException e) {
            LOGGER.error("Error reading historical log file {}: {}", file, e.getMessage());
        }
    }


    private void tailFile(Path containerDir, Path logFile) {
        String containerId = containerDir.getFileName().toString();
        String serviceName = resolveServiceName(containerDir);
        try (RandomAccessFile raf = new RandomAccessFile(logFile.toFile(), "r")) {
            String line;

            while (true) {
                line = raf.readLine();
                if (line == null) {
                    Thread.sleep(1000); // wait for new data
                    continue;
                }
                processLine(line, containerId, serviceName);
            }
        } catch (Exception e) {
            LOGGER.info("Stopped tailing " + logFile + ": " + e.getMessage());
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
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }
    }

    String resolveServiceName(Path containerDir) {
        try {
            Path configPath = containerDir.resolve("config.v2.json");

            if (Files.exists(configPath)) {
                JsonNode cfg = mapper.readTree(Files.readString(configPath));

                String serviceName = cfg.path("Config")
                        .path("Labels")
                        .path("com.msinit.service-name")
                        .asText();
                if (!serviceName.isEmpty())
                    return serviceName;

                return cfg.path("Name").asText("").replace("/", "");
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }

        return containerDir.getFileName().toString();
    }
}

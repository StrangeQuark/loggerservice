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
            Files.list(DOCKER_LOG_DIR)
                    .filter(Files::isDirectory)
                    .forEach(containerDir -> {
                        try {
                            Files.list(containerDir)
                                    .filter(f -> f.getFileName().toString().endsWith("-json.log"))
                                    .forEach(f -> startTail(containerDir, f));
                        } catch (IOException e) {
                            LOGGER.error("Error listing containerDir {}: {}", containerDir, e.getMessage());
                        }
                    });

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
        LOGGER.info("Container directory detected: " + containerDir);
        Path logFile = containerDir.resolve(containerDir.getFileName().toString() + "-json.log");

        if (activeTails.containsKey(logFile))
            return;

        // Watch the container directory for the log file if not present
        executor.submit(() -> {
            try {
                if (Files.exists(logFile)) {
                    startTail(containerDir, logFile);
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
                                startTail(containerDir, logFile);
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

    private void startTail(Path containerDir, Path logFile) {
        try {
            // Process all existing logs first
            try (Stream<Path> files = Files.list(containerDir)) {
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

    private String resolveServiceName(Path containerDir) {
        try {
            Path configPath = containerDir.resolve("config.v2.json");

            if (Files.exists(configPath)) {
                JsonNode cfg = mapper.readTree(Files.readString(configPath));
                return cfg.path("Name").asText("").replace("/", "");
            }
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
        }

        return containerDir.getFileName().toString();
    }
}

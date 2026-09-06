package com.strangequark.loggerservice;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class DockerLogTailerTest {

    @TempDir
    Path containerDir;

    @Test
    void labeledContainerIsCollected() throws Exception {
        Files.writeString(containerDir.resolve("config.v2.json"), """
                {
                  "Name": "/auth-service",
                  "Config": {
                    "Labels": {
                      "com.msinit.log": "true",
                      "com.msinit.service-name": "authservice"
                    }
                  }
                }
                """);

        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        assertTrue(dockerLogTailer.isLogCollectionEnabled(containerDir));
        assertEquals("authservice", dockerLogTailer.resolveServiceName(containerDir));
    }

    @Test
    void unlabeledContainerIsNotCollected() throws Exception {
        Files.writeString(containerDir.resolve("config.v2.json"), """
                {
                  "Name": "/unrelated-container",
                  "Config": {
                    "Labels": {}
                  }
                }
                """);

        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        assertFalse(dockerLogTailer.isLogCollectionEnabled(containerDir));
        assertEquals("unrelated-container", dockerLogTailer.resolveServiceName(containerDir));
    }

    @Test
    void missingConfigIsNotCollectedTest() {
        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        assertFalse(dockerLogTailer.isLogCollectionEnabled(containerDir));
    }

    @Test
    void invalidConfigIsNotCollectedTest() throws Exception {
        Files.writeString(containerDir.resolve("config.v2.json"), "invalid json");

        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        assertFalse(dockerLogTailer.isLogCollectionEnabled(containerDir));
    }

    @Test
    void sameLogLineHasSameId() {
        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        String logId = dockerLogTailer.getLogId("container-id", "{\"log\":\"test\"}");

        assertEquals(logId, dockerLogTailer.getLogId("container-id", "{\"log\":\"test\"}"));
        assertNotEquals(logId, dockerLogTailer.getLogId("container-id", "{\"log\":\"different\"}"));
    }

    @Test
    void differentContainerHasDifferentLogIdTest() {
        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        String logId = dockerLogTailer.getLogId("container-id", "{\"log\":\"test\"}");

        assertNotEquals(logId, dockerLogTailer.getLogId("different-container-id", "{\"log\":\"test\"}"));
    }

    @Test
    void stoppedContainerIsNotRunning() throws Exception {
        Files.writeString(containerDir.resolve("config.v2.json"), """
                {
                  "State": {
                    "Running": false
                  }
                }
                """);

        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        assertFalse(dockerLogTailer.isContainerRunning(containerDir));
    }

    @Test
    void runningContainerIsRunningTest() throws Exception {
        Files.writeString(containerDir.resolve("config.v2.json"), """
                {
                  "State": {
                    "Running": true
                  }
                }
                """);

        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        assertTrue(dockerLogTailer.isContainerRunning(containerDir));
    }

    @Test
    void replacedLogFileIsDetected() throws Exception {
        Path logFile = containerDir.resolve("container-id-json.log");
        Files.writeString(logFile, "old log");

        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);
        Object fileKey = dockerLogTailer.getFileKey(logFile);

        Files.move(logFile, containerDir.resolve("container-id-json.log.1"));
        Files.writeString(logFile, "new log");

        assertTrue(dockerLogTailer.hasLogFileChanged(logFile, fileKey));
    }

    @Test
    void unchangedLogFileIsNotDetectedAsReplacedTest() throws Exception {
        Path logFile = containerDir.resolve("container-id-json.log");
        Files.writeString(logFile, "log");

        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);
        Object fileKey = dockerLogTailer.getFileKey(logFile);

        assertFalse(dockerLogTailer.hasLogFileChanged(logFile, fileKey));
    }

    @Test
    void validDockerLogLineIsIndexedTest() {
        OpenSearchService openSearchService = mock(OpenSearchService.class);
        DockerLogTailer dockerLogTailer = new DockerLogTailer(openSearchService);
        String line = "{\"log\":\" test message\\n\",\"stream\":\"stderr\",\"time\":\"2026-09-06T12:00:00.000000000Z\"}";

        dockerLogTailer.processLine(line, "container-id", "test-service");

        ArgumentCaptor<LogEntry> entryCaptor = ArgumentCaptor.forClass(LogEntry.class);
        ArgumentCaptor<String> logIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(openSearchService).indexLog(entryCaptor.capture(), logIdCaptor.capture());
        assertEquals("container-id", entryCaptor.getValue().getContainerId());
        assertEquals("test-service", entryCaptor.getValue().getServiceName());
        assertEquals("stderr", entryCaptor.getValue().getStream());
        assertEquals("test message", entryCaptor.getValue().getMessage());
        assertEquals("2026-09-06T12:00:00Z", entryCaptor.getValue().getTimestamp().toString());
        assertEquals(dockerLogTailer.getLogId("container-id", line), logIdCaptor.getValue());
    }

    @Test
    void invalidDockerLogLineDoesNotIndexTest() {
        OpenSearchService openSearchService = mock(OpenSearchService.class);
        DockerLogTailer dockerLogTailer = new DockerLogTailer(openSearchService);

        dockerLogTailer.processLine("invalid json", "container-id", "test-service");

        verifyNoInteractions(openSearchService);
    }

    @Test
    void historicalLogLinesAreIndexedTest() throws Exception {
        Files.writeString(containerDir.resolve("config.v2.json"), """
                {
                  "Config": {
                    "Labels": {
                      "com.msinit.service-name": "test-service"
                    }
                  }
                }
                """);
        Path logFile = containerDir.resolve("container-id-json.log.1");
        Files.writeString(logFile, """
                {"log":"first\\n","stream":"stdout","time":"2026-09-06T12:00:00.000000000Z"}
                {"log":"second\\n","stream":"stderr","time":"2026-09-06T12:01:00.000000000Z"}
                """);

        OpenSearchService openSearchService = mock(OpenSearchService.class);
        DockerLogTailer dockerLogTailer = new DockerLogTailer(openSearchService);

        dockerLogTailer.processHistoricalLogs(containerDir, logFile);

        verify(openSearchService, org.mockito.Mockito.times(2)).indexLog(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }
}

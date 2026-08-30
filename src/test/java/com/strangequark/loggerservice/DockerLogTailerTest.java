package com.strangequark.loggerservice;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void sameLogLineHasSameId() {
        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);

        String logId = dockerLogTailer.getLogId("container-id", "{\"log\":\"test\"}");

        assertEquals(logId, dockerLogTailer.getLogId("container-id", "{\"log\":\"test\"}"));
        assertNotEquals(logId, dockerLogTailer.getLogId("container-id", "{\"log\":\"different\"}"));
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
    void replacedLogFileIsDetected() throws Exception {
        Path logFile = containerDir.resolve("container-id-json.log");
        Files.writeString(logFile, "old log");

        DockerLogTailer dockerLogTailer = new DockerLogTailer(null);
        Object fileKey = dockerLogTailer.getFileKey(logFile);

        Files.move(logFile, containerDir.resolve("container-id-json.log.1"));
        Files.writeString(logFile, "new log");

        assertTrue(dockerLogTailer.hasLogFileChanged(logFile, fileKey));
    }
}

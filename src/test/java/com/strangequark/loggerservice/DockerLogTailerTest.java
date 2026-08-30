package com.strangequark.loggerservice;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}

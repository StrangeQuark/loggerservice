package com.strangequark.loggerservice;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class KubernetesLogTailerTest {

    @Mock
    OpenSearchService openSearchService;

    @Captor
    ArgumentCaptor<LogEntry> logEntryCaptor;

    @Test
    void kubernetesLogIsIndexedTest() {
        KubernetesLogTailer kubernetesLogTailer = new KubernetesLogTailer(openSearchService);

        kubernetesLogTailer.processLogs("2026-09-18T13:00:00Z Application started", "pod-id:app", "authservice");

        verify(openSearchService).indexLog(logEntryCaptor.capture(), anyString());
        assertEquals("pod-id:app", logEntryCaptor.getValue().getContainerId());
        assertEquals("authservice", logEntryCaptor.getValue().getServiceName());
        assertEquals("Application started", logEntryCaptor.getValue().getMessage());
    }
}

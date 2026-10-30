package com.strangequark.loggerservice;

import org.junit.jupiter.api.Test;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class OpenSearchServiceTest {

    @Test
    void logSourceHandlesSpecialCharacters() {
        LogEntry entry = new LogEntry();
        Instant timestamp = Instant.parse("2026-08-30T12:00:00Z");
        entry.setContainerId("container-id");
        entry.setServiceName("logger-service");
        entry.setStream("stdout");
        entry.setMessage("Quote: \" Backslash: \\ Tab: \t Newline: \n Carriage return: \r Unicode: ✓");
        entry.setTimestamp(timestamp);

        Map<String, Object> source = new OpenSearchService().getLogSource(entry);

        assertEquals(entry.getMessage(), source.get("message"));
        assertEquals(timestamp.toString(), source.get("@timestamp"));
        assertFalse(source.containsKey("timestamp"));
    }

    @Test
    void logSourceContainsExpectedFieldsTest() {
        LogEntry entry = new LogEntry();
        entry.setContainerId("container-id");
        entry.setServiceName("test-service");
        entry.setStream("stdout");
        entry.setMessage("test message");
        entry.setTimestamp(Instant.parse("2026-09-06T12:00:00Z"));

        Map<String, Object> source = new OpenSearchService().getLogSource(entry);

        assertEquals("container-id", source.get("containerId"));
        assertEquals("test-service", source.get("serviceName"));
        assertEquals("stdout", source.get("stream"));
        assertEquals("test message", source.get("message"));
        assertEquals("2026-09-06T12:00:00Z", source.get("@timestamp"));
    }

    @Test
    void structuredLogTraceFieldsAreIndexedWithoutChangingMessageTest() {
        LogEntry entry = new LogEntry();
        entry.setMessage("{\"message\":\"request failed\",\"traceId\":\"4bf92f3577b34da6a3ce929d0e0e4736\",\"spanId\":\"00f067aa0ba902b7\",\"stack_trace\":\"error details\"}");
        entry.setTimestamp(Instant.now());

        Map<String, Object> source = new OpenSearchService().getLogSource(entry);

        assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", source.get("traceId"));
        assertEquals("00f067aa0ba902b7", source.get("spanId"));
        assertEquals(entry.getMessage(), source.get("message"));
    }

    @Test
    void malformedAndUntracedMessagesRemainSearchableTest() {
        for(String message : new String[]{"ordinary log", "{invalid json", "{\"message\":\"startup\"}",
                "{\"traceId\":\"00000000000000000000000000000000\",\"spanId\":\"0000000000000000\"}"}) {
            LogEntry entry = new LogEntry();
            entry.setMessage(message);
            entry.setTimestamp(Instant.now());

            Map<String, Object> source = new OpenSearchService().getLogSource(entry);

            assertEquals(message, source.get("message"));
            assertFalse(source.containsKey("traceId"));
            assertFalse(source.containsKey("spanId"));
        }
    }

    @Test
    void openSearchFailureDoesNotThrowTest() throws Exception {
        RestHighLevelClient client = mock(RestHighLevelClient.class);
        doThrow(new java.io.IOException("OpenSearch unavailable")).when(client).index(any(), any());

        OpenSearchService openSearchService = new OpenSearchService();
        ReflectionTestUtils.setField(openSearchService, "client", client);

        LogEntry entry = new LogEntry();
        entry.setContainerId("container-id");
        entry.setServiceName("test-service");
        entry.setStream("stdout");
        entry.setMessage("test message");
        entry.setTimestamp(Instant.now());

        assertDoesNotThrow(() -> openSearchService.indexLog(entry, "log-id"));
    }
}

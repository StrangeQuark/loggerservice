package com.strangequark.loggerservice;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
}

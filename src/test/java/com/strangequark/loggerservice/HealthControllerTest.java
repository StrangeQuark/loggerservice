package com.strangequark.loggerservice;

import com.strangequark.loggerservice.health.HealthController;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HealthControllerTest {

    @Test
    void healthcheckReturnsOk() {
        ResponseEntity<String> response = new HealthController().healthcheck();

        assertEquals(200, response.getStatusCode().value());
        assertEquals("200 OK", response.getBody());
    }
}

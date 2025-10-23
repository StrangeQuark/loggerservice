package com.strangequark.loggerservice;

import jakarta.annotation.PostConstruct;
import org.apache.http.HttpHost;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Service
public class DashboardsService {

    private static final String INDEX_NAME = "docker-logs";
    private static final String INDEX_PATTERN_ID = "docker-logs";
    private static final String INDEX_PATTERN_TITLE = "docker-logs";
    private static final String TIME_FIELD = "timestamp";

    private RestHighLevelClient osClient;
    private HttpClient httpClient;

    @PostConstruct
    public void init() {
        String osHost = System.getenv().getOrDefault("OPENSEARCH_HOST", "localhost");
        int osPort = Integer.parseInt(System.getenv().getOrDefault("OPENSEARCH_PORT", "9200"));
        String dashboardsHost = System.getenv().getOrDefault("DASHBOARDS_HOST", "dashboards");
        int dashboardsPort = Integer.parseInt(System.getenv().getOrDefault("DASHBOARDS_PORT", "5601"));

        osClient = new RestHighLevelClient(RestClient.builder(new HttpHost(osHost, osPort, "http")));
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        waitForService("OpenSearch", "http://" + osHost + ":" + osPort, 30);
        ensureIndexExists();

        waitForService("Dashboards", "http://" + dashboardsHost + ":" + dashboardsPort + "/api/status", 60);
        createIndexPattern(dashboardsHost, dashboardsPort);

        setDefaultIndexPattern(dashboardsHost, dashboardsPort);
    }

    private void waitForService(String name, String url, int timeoutSeconds) {
        System.out.println("Waiting for " + name + " to become ready...");
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutSeconds * 1000L) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() < 500) {
                    System.out.println(name + " is ready.");
                    return;
                }
            } catch (Exception ignored) {}
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
        }
        System.err.println("Warning: " + name + " did not become ready within " + timeoutSeconds + " seconds.");
    }

    private void ensureIndexExists() {
        try {
            GetIndexRequest getRequest = new GetIndexRequest(INDEX_NAME);
            boolean exists = osClient.indices().exists(getRequest, RequestOptions.DEFAULT);
            if (!exists) {
                System.out.println("Creating OpenSearch index: " + INDEX_NAME);
                CreateIndexRequest createRequest = new CreateIndexRequest(INDEX_NAME);
                createRequest.settings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                );
                createRequest.mapping("""
                    {
                      "properties": {
                        "containerId": { "type": "keyword" },
                        "serviceName": { "type": "keyword" },
                        "stream": { "type": "keyword" },
                        "message": { "type": "text" },
                        "@timestamp": { "type": "date" }
                      }
                    }
                """, XContentType.JSON);
                osClient.indices().create(createRequest, RequestOptions.DEFAULT);
            } else {
                System.out.println("Index already exists: " + INDEX_NAME);
            }
        } catch (IOException e) {
            System.err.println("Failed to ensure index exists:");
            e.printStackTrace();
        }
    }

    private void createIndexPattern(String dashboardsHost, int dashboardsPort) {
        String url = "http://" + dashboardsHost + ":" + dashboardsPort +
                "/api/saved_objects/index-pattern/" + INDEX_PATTERN_ID;

        String json = String.format("""
            {
              "attributes": {
                "title": "%s",
                "timeFieldName": "%s"
              }
            }
        """, INDEX_PATTERN_TITLE, TIME_FIELD);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("osd-xsrf", "true")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200 || response.statusCode() == 409) {
                System.out.println("Dashboards index pattern created or already exists: " + INDEX_PATTERN_ID);
            } else {
                System.err.printf("Failed to create Dashboards index pattern (HTTP %d): %s%n",
                        response.statusCode(), response.body());
            }
        } catch (Exception e) {
            System.err.println("Error creating Dashboards index pattern:");
            e.printStackTrace();
        }
    }

    private void setDefaultIndexPattern(String dashboardsHost, int dashboardsPort) {
        String url = "http://" + dashboardsHost + ":" + dashboardsPort + "/api/opensearch-dashboards/settings/defaultIndex";

        String json = String.format("""
            { "value": "%s" }
        """, INDEX_PATTERN_ID);

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("osd-xsrf", "true")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                System.out.println("Default index pattern set to: " + INDEX_PATTERN_ID);
            } else {
                System.err.printf("Failed to set default index pattern (HTTP %d): %s%n",
                        response.statusCode(), response.body());
            }
        } catch (Exception e) {
            System.err.println("Error setting default index pattern:");
            e.printStackTrace();
        }
    }
}

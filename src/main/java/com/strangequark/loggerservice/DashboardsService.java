package com.strangequark.loggerservice;

import jakarta.annotation.PostConstruct;
import org.apache.http.auth.AuthScope;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

@Service
public class DashboardsService {

    private static final String INDEX_NAME = "docker-logs";
    private static final String INDEX_PATTERN_ID = "docker-logs";
    private static final String INDEX_PATTERN_TITLE = "docker-logs";
    private static final String TIME_FIELD = "@timestamp";

    private RestHighLevelClient osClient;
    private HttpClient httpClient;
    private String authorization;

    @Value("${opensearch.host}")
    private String osHost;

    @Value("${opensearch.port}")
    private int osPort;

    @Value("${opensearch.username}")
    private String username;

    @Value("${opensearch.password}")
    private String password;

    @Value("${opensearch.ssl.verification-mode}")
    private String verificationMode;

    @Value("${dashboards.host}")
    private String dashboardsHost;

    @Value("${dashboards.port}")
    private int dashboardsPort;

    @Value("${service.http.connect.timeout}")
    private int connectTimeout;

    @Value("${service.http.read.timeout}")
    private int readTimeout;

    @PostConstruct
    public void init() {
        authorization = "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        try {
            if (verificationMode.equals("none")) {
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial((chain, authType) -> true)
                        .build();

                osClient = new RestHighLevelClient(
                        RestClient.builder(new HttpHost(osHost, osPort, "https"))
                                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                                        .setConnectTimeout(connectTimeout)
                                        .setSocketTimeout(readTimeout)
                                )
                                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)
                                        .setSSLContext(sslContext)
                                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                )
                );
            } else {
                osClient = new RestHighLevelClient(
                        RestClient.builder(new HttpHost(osHost, osPort, "https"))
                                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                                        .setConnectTimeout(connectTimeout)
                                        .setSocketTimeout(readTimeout)
                                )
                                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)
                                )
                );
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        HttpClient.Builder httpClientBuilder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5));
        if (verificationMode.equals("none")) {
            httpClientBuilder.sslContext(createSslContext());
        }
        httpClient = httpClientBuilder.build();

        waitForService("OpenSearch", "https://" + osHost + ":" + osPort, 30);
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
                        .header("Authorization", authorization)
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
                    .header("Authorization", authorization)
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
                    .header("Authorization", authorization)
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

    private SSLContext createSslContext() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }}, new SecureRandom());
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

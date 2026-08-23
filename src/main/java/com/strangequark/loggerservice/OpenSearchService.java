package com.strangequark.loggerservice;

import org.apache.http.auth.AuthScope;
import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import javax.net.ssl.SSLContext;

@Service
public class OpenSearchService {
    private RestHighLevelClient client;

    @PostConstruct
    public void init() {
        String host = System.getenv().getOrDefault("OPENSEARCH_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("OPENSEARCH_PORT", "9200"));
        String username = System.getenv().getOrDefault("OPENSEARCH_USERNAME", "admin");
        String password = System.getenv().getOrDefault("OPENSEARCH_PASSWORD", "");
        String verificationMode = System.getenv().getOrDefault("OPENSEARCH_SSL_VERIFICATIONMODE", "full");

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        try {
            if (verificationMode.equals("none")) {
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial((chain, authType) -> true)
                        .build();

                client = new RestHighLevelClient(
                        RestClient.builder(new HttpHost(host, port, "https"))
                                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)
                                        .setSSLContext(sslContext)
                                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                )
                );
            } else {
                client = new RestHighLevelClient(
                        RestClient.builder(new HttpHost(host, port, "https"))
                                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                                        .setDefaultCredentialsProvider(credentialsProvider)
                                )
                );
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void indexLog(LogEntry entry) {
        try {
            IndexRequest request = new IndexRequest("docker-logs")
                    .source(
                            String.format("{\"containerId\":\"%s\",\"serviceName\":\"%s\",\"stream\":\"%s\",\"message\":%s,\"timestamp\":\"%s\"}",
                                    entry.getContainerId(),
                                    entry.getServiceName(),
                                    entry.getStream(),
                                    escapeJson(entry.getMessage()),
                                    entry.getTimestamp()
                            ),
                            XContentType.JSON
                    );
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String escapeJson(String s) {
        return "\"" + s.replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
}

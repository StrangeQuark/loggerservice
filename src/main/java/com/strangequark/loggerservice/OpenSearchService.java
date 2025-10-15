package com.strangequark.loggerservice;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import java.io.IOException;

@Service
public class OpenSearchService {
    private RestHighLevelClient client;

    @PostConstruct
    public void init() {
        String host = System.getenv().getOrDefault("OPENSEARCH_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("OPENSEARCH_PORT", "9200"));

        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, "http"))
        );
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

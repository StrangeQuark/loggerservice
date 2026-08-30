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
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;

@Service
public class OpenSearchService {
    private RestHighLevelClient client;

    @Value("${opensearch.host}")
    private String host;

    @Value("${opensearch.port}")
    private int port;

    @Value("${opensearch.username}")
    private String username;

    @Value("${opensearch.password}")
    private String password;

    @Value("${opensearch.ssl.verification-mode}")
    private String verificationMode;

    @Value("${service.http.connect.timeout}")
    private int connectTimeout;

    @Value("${service.http.read.timeout}")
    private int readTimeout;

    @PostConstruct
    public void init() {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        try {
            if (verificationMode.equals("none")) {
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial((chain, authType) -> true)
                        .build();

                client = new RestHighLevelClient(
                        RestClient.builder(new HttpHost(host, port, "https"))
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
                client = new RestHighLevelClient(
                        RestClient.builder(new HttpHost(host, port, "https"))
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
    }

    public void indexLog(LogEntry entry, String logId) {
        try {
            IndexRequest request = new IndexRequest("docker-logs")
                    .id(logId)
                    .source(getLogSource(entry));
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Map<String, Object> getLogSource(LogEntry entry) {
        Map<String, Object> source = new HashMap<>();
        source.put("containerId", entry.getContainerId());
        source.put("serviceName", entry.getServiceName());
        source.put("stream", entry.getStream());
        source.put("message", entry.getMessage());
        source.put("@timestamp", entry.getTimestamp().toString());
        return source;
    }
}

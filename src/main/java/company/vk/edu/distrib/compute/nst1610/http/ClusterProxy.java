package company.vk.edu.distrib.compute.nst1610.http;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class ClusterProxy {
    public static final String INTERNAL_PROXY_HEADER = "X-Nst1610-Internal-Proxy";

    private final HttpClient httpClient = HttpClient.newHttpClient();

    public ProxyResponse forward(String endpoint, String id, int ack, HttpExchange exchange)
        throws IOException, InterruptedException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(
                endpoint
                    + "/v0/entity?id=" + URLEncoder.encode(id, StandardCharsets.UTF_8)
                    + "&ack=" + ack
            ))
            .timeout(Duration.ofSeconds(2))
            .header(INTERNAL_PROXY_HEADER, "true");
        switch (exchange.getRequestMethod()) {
            case "GET" -> requestBuilder.GET();
            case "PUT" -> requestBuilder.PUT(
                HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes())
            );
            case "DELETE" -> requestBuilder.DELETE();
            default -> throw new IllegalArgumentException("Unsupported method: " + exchange.getRequestMethod());
        }
        HttpResponse<byte[]> response = httpClient.send(
            requestBuilder.build(),
            HttpResponse.BodyHandlers.ofByteArray()
        );
        return new ProxyResponse(response.statusCode(), response.body());
    }

    public record ProxyResponse(int statusCode, byte[] body) {
    }
}

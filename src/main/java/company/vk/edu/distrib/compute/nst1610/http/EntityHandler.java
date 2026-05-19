package company.vk.edu.distrib.compute.nst1610.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.nst1610.replication.ReplicatedFileStorage;
import company.vk.edu.distrib.compute.nst1610.sharding.HashingStrategy;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);

    private final ReplicatedFileStorage replicatedStorage;
    private final String localEndpoint;
    private final HashingStrategy strategy;
    private final ClusterProxy clusterProxy;
    private final AuditSink auditSink;

    public EntityHandler(
        ReplicatedFileStorage replicatedStorage,
        String localEndpoint,
        HashingStrategy strategy,
        ClusterProxy clusterProxy,
        AuditSink auditSink
    ) {
        this.replicatedStorage = replicatedStorage;
        this.localEndpoint = localEndpoint;
        this.strategy = strategy;
        this.clusterProxy = clusterProxy;
        this.auditSink = auditSink;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                handleRequest(exchange);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, 0);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, 0);
            } catch (IOException e) {
                log.error("I/O error while handling entity request", e);
                exchange.sendResponseHeaders(500, 0);
            } catch (RuntimeException e) {
                log.error("Unexpected error while handling entity request", e);
                exchange.sendResponseHeaders(500, 0);
            }
        }
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        long requestTimestamp = System.currentTimeMillis();
        String id = extractId(exchange.getRequestURI());
        int ack = extractAck(exchange.getRequestURI());
        if (id == null) {
            exchange.sendResponseHeaders(400, 0);
            return;
        }
        if (!isInternalProxyRequest(exchange)) {
            auditSink.publish(new AuditEvent(exchange.getRequestMethod(), id, requestTimestamp));
        }
        if (ack <= 0 || ack > replicatedStorage.numberOfReplicas()) {
            exchange.sendResponseHeaders(400, 0);
            return;
        }
        if (!isSupportedMethod(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, 0);
            return;
        }
        String targetEndpoint = strategy.resolve(id);
        if (!localEndpoint.equals(targetEndpoint)) {
            proxyRequest(exchange, id, ack, targetEndpoint);
            return;
        }
        handleByMethod(exchange, id, ack);
    }

    private void proxyRequest(HttpExchange exchange, String id, int ack, String endpoint) throws IOException {
        try {
            ClusterProxy.ProxyResponse response = clusterProxy.forward(endpoint, id, ack, exchange);
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            if (response.body().length > 0) {
                exchange.getResponseBody().write(response.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Proxy request interrupted", e);
        }
    }

    private void handleByMethod(HttpExchange exchange, String id, int ack) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> handleGet(exchange, id, ack);
            case "PUT" -> handlePut(exchange, id, ack);
            case "DELETE" -> handleDelete(exchange, id, ack);
            default -> exchange.sendResponseHeaders(405, 0);
        }
    }

    private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
        ReplicatedFileStorage.ReadResult result = replicatedStorage.get(id, ack);
        if (!result.enoughReplicas()) {
            exchange.sendResponseHeaders(500, 0);
            return;
        }
        if (!result.found()) {
            exchange.sendResponseHeaders(404, 0);
            return;
        }
        exchange.sendResponseHeaders(200, result.value().length);
        exchange.getResponseBody().write(result.value());
    }

    private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
        boolean replicated = replicatedStorage.put(id, exchange.getRequestBody().readAllBytes(), ack);
        exchange.sendResponseHeaders(replicated ? 201 : 500, 0);
    }

    private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
        boolean replicated = replicatedStorage.delete(id, ack);
        exchange.sendResponseHeaders(replicated ? 202 : 500, 0);
    }

    private boolean isSupportedMethod(String method) {
        return "GET".equals(method) || "PUT".equals(method) || "DELETE".equals(method);
    }

    private boolean isInternalProxyRequest(HttpExchange exchange) {
        return "true".equalsIgnoreCase(
            exchange.getRequestHeaders().getFirst(ClusterProxy.INTERNAL_PROXY_HEADER)
        );
    }

    private String extractId(URI uri) {
        String value = extractQueryParam(uri, "id");
        if (value == null || value.isEmpty()) {
            return null;
        }
        String decodedValue = URLDecoder.decode(value, StandardCharsets.UTF_8);
        return decodedValue.isEmpty() ? null : decodedValue;
    }

    private Integer extractAck(URI uri) {
        String ackValue = extractQueryParam(uri, "ack");
        if (ackValue == null) {
            return 1;
        }
        return Integer.parseInt(ackValue);
    }

    private String extractQueryParam(URI uri, String targetParam) {
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return null;
        }
        for (String param : query.split("&")) {
            String prefix = targetParam + "=";
            if (param.startsWith(prefix)) {
                return param.substring(prefix.length());
            }
        }
        return null;
    }

    @FunctionalInterface
    public interface AuditSink {
        void publish(AuditEvent event);
    }
}

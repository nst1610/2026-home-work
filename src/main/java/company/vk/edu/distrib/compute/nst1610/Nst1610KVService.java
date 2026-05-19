package company.vk.edu.distrib.compute.nst1610;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.nst1610.audit.KafkaAuditPublisher;
import company.vk.edu.distrib.compute.nst1610.http.ClusterProxy;
import company.vk.edu.distrib.compute.nst1610.http.EntityHandler;
import company.vk.edu.distrib.compute.nst1610.http.StatusHandler;
import company.vk.edu.distrib.compute.nst1610.replication.ReplicatedFileStorage;
import company.vk.edu.distrib.compute.nst1610.sharding.HashingStrategy;
import company.vk.edu.distrib.compute.nst1610.sharding.RendezvousHashingStrategy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Nst1610KVService implements AuditableKVService, ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(Nst1610KVService.class);
    private static final int DEFAULT_REPLICATION_FACTOR = 9;
    private static final String REPLICATION_FACTOR_ENV = "NST1610_REPLICATION_FACTOR";
    private final int servicePort;
    private final HttpServer server;
    private final String localEndpoint;
    private final HashingStrategy strategy;
    private final ClusterProxy clusterProxy;
    private final ReplicatedFileStorage replicatedStorage;
    private final Object auditPublisherLock = new Object();
    private String bootstrapServers;
    private boolean asyncMode = true;
    private Optional<KafkaAuditPublisher> auditPublisher = Optional.empty();

    public Nst1610KVService(int port) throws IOException {
        this(port, List.of(getEndpoint(port)), getEndpoint(port), resolveReplicationFactor());
    }

    public Nst1610KVService(int port, List<String> clusterEndpoints, String localEndpoint) throws IOException {
        this(port, clusterEndpoints, localEndpoint, resolveReplicationFactor());
    }

    public Nst1610KVService(int port, List<String> clusterEndpoints, String localEndpoint, int replicationFactor)
        throws IOException {
        this.servicePort = port;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.localEndpoint = localEndpoint;
        this.strategy = new RendezvousHashingStrategy();
        this.strategy.updateEndpoints(clusterEndpoints);
        this.clusterProxy = new ClusterProxy();
        this.replicatedStorage = new ReplicatedFileStorage(
            Path.of("storage", Integer.toString(port)),
            replicationFactor
        );
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler(
                replicatedStorage,
                localEndpoint,
                strategy,
                clusterProxy,
                event -> publisher().publish(event)
            )
        );
    }

    @Override
    public void start() {
        log.info("Server start");
        server.start();
    }

    @Override
    public void stop() {
        log.info("Server stop");
        server.stop(0);
        KafkaAuditPublisher publisher = detachPublisher();
        if (publisher != null) {
            publisher.close();
        }
    }

    private static String getEndpoint(int port) {
        return "http://localhost:" + port;
    }

    public void updateClusterEndpoints(List<String> clusterEndpoints) {
        strategy.updateEndpoints(clusterEndpoints);
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public int numberOfReplicas() {
        return replicatedStorage.numberOfReplicas();
    }

    @Override
    public void disableReplica(int nodeId) {
        replicatedStorage.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicatedStorage.enableReplica(nodeId);
    }

    @Override
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        KafkaAuditPublisher publisher = detachPublisher();
        if (publisher != null) {
            publisher.close();
        }
    }

    @Override
    public void setAsync(boolean enabled) {
        this.asyncMode = enabled;
        KafkaAuditPublisher publisher = currentPublisher();
        if (publisher != null) {
            publisher.setAsyncMode(enabled);
        }
    }

    private KafkaAuditPublisher publisher() {
        synchronized (auditPublisherLock) {
            if (auditPublisher.isEmpty()) {
                KafkaAuditPublisher createdPublisher = new KafkaAuditPublisher(bootstrapServers);
                createdPublisher.setAsyncMode(asyncMode);
                auditPublisher = Optional.of(createdPublisher);
            }
            return auditPublisher.orElseThrow();
        }
    }

    private KafkaAuditPublisher currentPublisher() {
        synchronized (auditPublisherLock) {
            return auditPublisher.orElse(null);
        }
    }

    private KafkaAuditPublisher detachPublisher() {
        synchronized (auditPublisherLock) {
            KafkaAuditPublisher publisher = auditPublisher.orElse(null);
            auditPublisher = Optional.empty();
            return publisher;
        }
    }

    private static int resolveReplicationFactor() {
        String envValue = System.getenv(REPLICATION_FACTOR_ENV);
        if (envValue != null && !envValue.isBlank()) {
            return validateReplicationFactor(Integer.parseInt(envValue));
        }
        return DEFAULT_REPLICATION_FACTOR;
    }

    private static int validateReplicationFactor(int replicationFactor) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }
        return replicationFactor;
    }
}

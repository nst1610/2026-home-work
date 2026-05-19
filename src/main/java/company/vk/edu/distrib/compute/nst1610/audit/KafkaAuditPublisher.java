package company.vk.edu.distrib.compute.nst1610.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAuditPublisher implements AutoCloseable {
    public static final String AUDIT_TOPIC = "audit";
    private static final Logger log = LoggerFactory.getLogger(KafkaAuditPublisher.class);
    private final String bootstrapServers;
    private final Lock producerLock = new ReentrantLock();
    private boolean asyncMode = true;
    private KafkaProducer<String, String> producer;

    public KafkaAuditPublisher(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public synchronized void setAsyncMode(boolean asyncMode) {
        this.asyncMode = asyncMode;
    }

    public void publish(AuditEvent event) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            return;
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(
            AUDIT_TOPIC,
            event.id(),
            AuditEventUtils.encode(event)
        );
        KafkaProducer<String, String> localProducer = producer();
        if (isAsyncMode()) {
            localProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to publish audit event", exception);
                }
            });
            return;
        }
        try {
            localProducer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing audit event", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to publish audit event", e);
        }
    }

    @Override
    public void close() {
        KafkaProducer<String, String> localProducer;
        producerLock.lock();
        try {
            localProducer = producer;
            producer = null;
        } finally {
            producerLock.unlock();
        }
        if (localProducer != null) {
            localProducer.flush();
            localProducer.close();
        }
    }

    private KafkaProducer<String, String> producer() {
        producerLock.lock();
        try {
            if (producer == null) {
                producer = new KafkaProducer<>(producerProperties());
            }
            return producer;
        } finally {
            producerLock.unlock();
        }
    }

    private synchronized boolean isAsyncMode() {
        return asyncMode;
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }
}

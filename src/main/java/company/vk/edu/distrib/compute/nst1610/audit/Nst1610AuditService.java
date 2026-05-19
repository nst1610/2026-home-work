package company.vk.edu.distrib.compute.nst1610.audit;

import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Nst1610AuditService implements AuditService {
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private final String bootstrapServers;
    private final String consumerGroupId;
    private final Path storageFile;
    private final List<AuditEvent> events;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Lock runtimeStateLock = new ReentrantLock();
    private Optional<KafkaConsumer<String, String>> consumer = Optional.empty();
    private Optional<Thread> consumerThread = Optional.empty();

    public Nst1610AuditService(String bootstrapServers, String consumerGroupId) throws IOException {
        this.bootstrapServers = bootstrapServers;
        this.consumerGroupId = consumerGroupId;
        this.storageFile = createStorageFile(consumerGroupId);
        this.events = new CopyOnWriteArrayList<>(loadStoredEvents(storageFile));
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties());
        kafkaConsumer.subscribe(List.of(KafkaAuditPublisher.AUDIT_TOPIC));
        setConsumer(kafkaConsumer);
        Thread thread = new Thread(() -> consume(kafkaConsumer), "nst1610-audit-" + consumerGroupId);
        thread.setDaemon(true);
        setConsumerThread(thread);
        thread.start();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        KafkaConsumer<String, String> kafkaConsumer = currentConsumer();
        if (kafkaConsumer != null) {
            kafkaConsumer.wakeup();
        }
        Thread thread = currentConsumerThread();
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while stopping audit service", e);
            }
        }
        clearRuntimeState();
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return List.copyOf(events);
    }

    private void consume(KafkaConsumer<String, String> kafkaConsumer) {
        try (kafkaConsumer) {
            while (running.get()) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    continue;
                }
                appendRecords(records);
                kafkaConsumer.commitSync();
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
        }
    }

    private void appendRecords(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            AuditEvent event = AuditEventUtils.decode(record.value());
            events.add(event);
            appendToFile(event);
        }
    }

    private void appendToFile(AuditEvent event) {
        try {
            Files.writeString(
                storageFile,
                AuditEventUtils.encode(event) + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            throw new IllegalStateException("Failed to persist audit event", e);
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    private static Path createStorageFile(String consumerGroupId) throws IOException {
        Path directory = Path.of("storage", "audit");
        Files.createDirectories(directory);
        String fileName = consumerGroupId + ".log";
        Path file = directory.resolve(fileName);
        if (!Files.exists(file)) {
            Files.createFile(file);
        }
        return file;
    }

    private static List<AuditEvent> loadStoredEvents(Path storageFile) throws IOException {
        if (!Files.exists(storageFile)) {
            return List.of();
        }
        List<AuditEvent> storedEvents = new ArrayList<>();
        for (String line : Files.readAllLines(storageFile, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                storedEvents.add(AuditEventUtils.decode(line));
            }
        }
        return storedEvents;
    }

    private KafkaConsumer<String, String> currentConsumer() {
        runtimeStateLock.lock();
        try {
            return consumer.orElse(null);
        } finally {
            runtimeStateLock.unlock();
        }
    }

    private Thread currentConsumerThread() {
        runtimeStateLock.lock();
        try {
            return consumerThread.orElse(null);
        } finally {
            runtimeStateLock.unlock();
        }
    }

    private void setConsumer(KafkaConsumer<String, String> kafkaConsumer) {
        runtimeStateLock.lock();
        try {
            consumer = Optional.of(kafkaConsumer);
        } finally {
            runtimeStateLock.unlock();
        }
    }

    private void setConsumerThread(Thread thread) {
        runtimeStateLock.lock();
        try {
            consumerThread = Optional.of(thread);
        } finally {
            runtimeStateLock.unlock();
        }
    }

    private void clearRuntimeState() {
        runtimeStateLock.lock();
        try {
            consumer = Optional.empty();
            consumerThread = Optional.empty();
        } finally {
            runtimeStateLock.unlock();
        }
    }
}

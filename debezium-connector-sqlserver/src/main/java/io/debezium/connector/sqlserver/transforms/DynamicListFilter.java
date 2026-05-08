package io.debezium.connector.sqlserver.transforms;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DynamicListFilter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicListFilter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static final String CONFIG_TOPIC_CONFIG = "config.topic";
    static final String FIELD_NAME_CONFIG = "field.name";
    static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    static final String EMPTY_LIST_BEHAVIOR_CONFIG = "empty.list.behavior";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONFIG_TOPIC_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Kafka topic that publishes the allowed field values as a JSON array")
            .define(FIELD_NAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Field in value.before/value.after to evaluate (e.g., com_id)")
            .define(BOOTSTRAP_SERVERS_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Kafka bootstrap servers used by the allow-list consumer (e.g., localhost:9092)")
            .define(EMPTY_LIST_BEHAVIOR_CONFIG, Type.STRING, "pass_all", Importance.MEDIUM, "Behavior when the allow-list is empty: 'pass_all' (default) or 'block_all'");

    private static final ConcurrentHashMap<String, AtomicReference<Set<Object>>> TOPIC_REGISTRY = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Thread> CONSUMER_THREADS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Integer> INSTANCE_COUNTS = new ConcurrentHashMap<>();

    private String configTopic;
    private String fieldName;
    private boolean passAllWhenEmpty;
    private AtomicReference<Set<Object>> allowedValues;

    @Override
    public void configure(Map<String, ?> configs) {
        configTopic = (String) configs.get(CONFIG_TOPIC_CONFIG);
        fieldName = (String) configs.get(FIELD_NAME_CONFIG);
        String emptyBehavior = configs.containsKey(EMPTY_LIST_BEHAVIOR_CONFIG)
                ? (String) configs.get(EMPTY_LIST_BEHAVIOR_CONFIG)
                : "pass_all";
        passAllWhenEmpty = "pass_all".equalsIgnoreCase(emptyBehavior);
        String bootstrapServers = (String) configs.get(BOOTSTRAP_SERVERS_CONFIG);

        allowedValues = TOPIC_REGISTRY.computeIfAbsent(configTopic, k -> new AtomicReference<>(Collections.emptySet()));

        CONSUMER_THREADS.computeIfAbsent(configTopic, topic -> {
            Thread t = new Thread(() -> runConsumer(bootstrapServers, topic), "debezium-dynamic-filter-" + topic);
            t.setDaemon(true);
            t.start();
            LOGGER.info("Started dynamic allow-list consumer for topic '{}'", topic);
            return t;
        });

        INSTANCE_COUNTS.merge(configTopic, 1, Integer::sum);
    }

    private void runConsumer(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        // Unique group ID so every startup reads from the earliest offset and replays all history
        props.put("group.id", "debezium-dynamic-filter-" + topic + "-" + UUID.randomUUID());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    AtomicReference<Set<Object>> ref = TOPIC_REGISTRY.getOrDefault(topic, allowedValues);
                    if (record.value() == null) {
                        ref.set(Collections.emptySet());
                        LOGGER.info("Cleared dynamic allow-list for topic '{}'", topic);
                    }
                    else {
                        updateAllowedValues(ref, topic, record.value());
                    }
                }
            }
        }
        catch (Exception e) {
            if (!Thread.currentThread().isInterrupted()) {
                LOGGER.error("Dynamic filter consumer error for topic '{}': {}", topic, e.getMessage(), e);
            }
        }
    }

    private void updateAllowedValues(AtomicReference<Set<Object>> ref, String topic, String json) {
        try {
            List<Object> list = OBJECT_MAPPER.readValue(json, new TypeReference<List<Object>>() {});
            Set<Object> newSet = new HashSet<>(list);
            ref.set(newSet);
            LOGGER.info("Updated dynamic allow-list for topic '{}' field '{}': {} value(s) → {}",
                    topic, fieldName, newSet.size(), newSet);
        }
        catch (Exception e) {
            LOGGER.warn("Failed to parse allow-list JSON '{}' from topic '{}': {}", json, topic, e.getMessage());
        }
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        Set<Object> current = allowedValues.get();
        if (current.isEmpty()) {
            return passAllWhenEmpty ? record : null;
        }

        if (!(record.value() instanceof Struct)) {
            return record;
        }

        Struct envelope = (Struct) record.value();

        Struct after = extractStruct(envelope, "after");
        if (after != null && fieldMatches(after, current)) {
            return record;
        }

        Struct before = extractStruct(envelope, "before");
        if (before != null && fieldMatches(before, current)) {
            return record;
        }

        if (after == null && before == null) {
            return record;
        }

        return null;
    }

    private boolean fieldMatches(Struct struct, Set<Object> allowed) {
        if (struct.schema().field(fieldName) == null) {
            return false;
        }
        return allowed.contains(struct.get(fieldName));
    }

    private Struct extractStruct(Struct envelope, String fieldName) {
        if (envelope.schema().field(fieldName) == null) {
            return null;
        }
        Object val = envelope.get(fieldName);
        return val instanceof Struct ? (Struct) val : null;
    }

    @Override
    public void close() {
        int remaining = INSTANCE_COUNTS.merge(configTopic, -1, Integer::sum);
        if (remaining <= 0) {
            INSTANCE_COUNTS.remove(configTopic);
            Thread t = CONSUMER_THREADS.remove(configTopic);
            if (t != null) {
                t.interrupt();
            }
            TOPIC_REGISTRY.remove(configTopic);
            LOGGER.info("Stopped dynamic allow-list consumer for topic '{}'", configTopic);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}

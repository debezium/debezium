/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.document.Document;
import io.debezium.document.DocumentSerdes;
import io.strimzi.test.container.StrimziKafkaCluster;

public class KafkaClusterUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClusterUtils.class);

    /**
     * Create a Kafka topic using AdminClient.
     *
     * @param topicName the name of the topic to create
     * @param partitions the number of partitions
     * @param replicationFactor the replication factor
     * @throws Exception if topic creation fails
     */
    public static void createTopic(String topicName, int partitions, short replicationFactor, String bootstrapServers) throws Exception {
        Properties adminProperties = new Properties();
        adminProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminProperties.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, "test-admin-client-" + UUID.randomUUID().toString());

        try (AdminClient adminClient = AdminClient.create(adminProperties)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

            try {
                CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
                result.all().get(30, TimeUnit.SECONDS);
                LOGGER.info("Topic '{}' created successfully", topicName);
            }
            catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    LOGGER.info("Topic '{}' already exists", topicName);
                }
                else {
                    throw e;
                }
            }
        }
    }

    /**
     * Get a new set of properties for consumers that want to talk to this server.
     *
     * @param groupId the group ID for the consumer; may not be null
     * @param clientId the optional identifier for the client; may be null if not needed
     * @param autoOffsetReset how to pick a starting offset when there is no initial offset or if an offset is
     *            out of range; may be null for the default to be used
     * @return the mutable consumer properties
     */
    public static Properties getConsumerProperties(String groupId, String clientId, OffsetResetStrategy autoOffsetReset,
                                                   StrimziKafkaCluster kafkaCluster) {
        if (groupId == null) {
            throw new IllegalArgumentException("The groupId is required");
        }
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        if (autoOffsetReset != null) {
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.toString().toLowerCase());
        }
        if (clientId != null) {
            props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        }
        return props;
    }

    /**
     * Get a new set of properties for producers that want to talk to this server.
     *
     * @param clientId the optional identifier for the client; may be null if not needed
     * @return the mutable producer properties
     */
    public static Properties getProducerProperties(String clientId, StrimziKafkaCluster kafkaCluster) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
        props.setProperty(ProducerConfig.ACKS_CONFIG, Integer.toString(1));
        if (clientId != null) {
            props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }
        return props;
    }

    /**
     * Create an {@link KafkaCluster.InteractiveProducer simple producer} that can be used to write messages to the cluster.
     *
     * @param producerName the name of the producer; may not be null
     * @param keySerializer the serializer for the keys; may not be null
     * @param valueSerializer the serializer for the values; may not be null
     * @return the object that can be used to produce messages; never null
     */
    public static <K, V> KafkaCluster.InteractiveProducer<K, V> createProducer(String producerName, Serializer<K> keySerializer,
                                                                               Serializer<V> valueSerializer, StrimziKafkaCluster kafkaCluster) {
        Properties props = getProducerProperties(producerName, kafkaCluster);
        KafkaProducer<K, V> producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
        return new KafkaCluster.InteractiveProducer<K, V>() {
            @Override
            public KafkaCluster.InteractiveProducer<K, V> write(ProducerRecord<K, V> record) {
                producer.send(record);
                producer.flush();
                return this;
            }

            @Override
            public void close() {
                producer.close();
            }
        };
    }

    /**
     * Create an {@link KafkaCluster.InteractiveProducer simple producer} that can be used to write {@link Document} messages to the
     * cluster.
     *
     * @param producerName the name of the producer; may not be null
     * @return the object that can be used to produce messages; never null
     */
    public static KafkaCluster.InteractiveProducer<String, Document> createProducer(String producerName, StrimziKafkaCluster kafkaCluster) {
        return createProducer(producerName, new StringSerializer(), new DocumentSerdes(), kafkaCluster);
    }

    /**
     * Create an {@link KafkaCluster.InteractiveConsumer simple consumer} that can be used to read messages from the cluster.
     *
     * @param groupId the name of the group; may not be null
     * @param clientId the name of the client; may not be null
     * @param topicName the name of the topic to read; may not be null and may not be empty
     * @param keyDeserializer the deserializer for the keys; may not be null
     * @param valueDeserializer the deserializer for the values; may not be null
     * @param completion the function to call when the consumer terminates; may be null
     * @return the running interactive consumer; never null
     */
    public static <K, V> KafkaCluster.InteractiveConsumer<K, V> createConsumer(String groupId, String clientId, String topicName,
                                                                               Deserializer<K> keyDeserializer,
                                                                               Deserializer<V> valueDeserializer, Runnable completion,
                                                                               StrimziKafkaCluster kafkaCluster) {
        Set<String> topicNames = Collections.singleton(topicName);
        return createConsumer(groupId, clientId, topicNames, keyDeserializer, valueDeserializer, completion, kafkaCluster);
    }

    /**
     * Create an {@link KafkaCluster.InteractiveConsumer simple consumer} that can be used to read messages from the cluster.
     *
     * @param groupId the name of the group; may not be null
     * @param clientId the name of the client; may not be null
     * @param topicNames the names of the topics to read; may not be null and may not be empty
     * @param keyDeserializer the deserializer for the keys; may not be null
     * @param valueDeserializer the deserializer for the values; may not be null
     * @param completion the function to call when the consumer terminates; may be null
     * @return the running interactive consumer; never null
     */
    public static <K, V> KafkaCluster.InteractiveConsumer<K, V> createConsumer(String groupId, String clientId, Set<String> topicNames,
                                                                               Deserializer<K> keyDeserializer,
                                                                               Deserializer<V> valueDeserializer, Runnable completion,
                                                                               StrimziKafkaCluster kafkaCluster) {
        BlockingQueue<ConsumerRecord<K, V>> consumed = new LinkedBlockingQueue<>();
        List<ConsumerRecord<K, V>> allMessages = new LinkedList<>();
        AtomicBoolean keepReading = new AtomicBoolean(true);
        OffsetCommitCallback offsetCommitCallback = null;
        consume(groupId, clientId, OffsetResetStrategy.EARLIEST, keyDeserializer, valueDeserializer, () -> keepReading.get(),
                offsetCommitCallback, completion, topicNames, record -> {
                    consumed.add(record);
                    allMessages.add(record);
                }, kafkaCluster);
        return new KafkaCluster.InteractiveConsumer<K, V>() {
            @Override
            public ConsumerRecord<K, V> nextRecord() throws InterruptedException {
                return consumed.take();
            }

            @Override
            public ConsumerRecord<K, V> nextRecord(long timeout, TimeUnit unit) throws InterruptedException {
                return consumed.poll(timeout, unit);
            }

            @Override
            public void close() {
                keepReading.set(false);
            }

            @Override
            public Stream<ConsumerRecord<K, V>> stream() {
                return consumed.stream();
            }

            @Override
            public Stream<ConsumerRecord<K, V>> streamAll() {
                return allMessages.stream();
            }
        };
    }

    /**
     * Create an {@link KafkaCluster.InteractiveConsumer simple consumer} that can be used to read messages from the cluster.
     *
     * @param groupId the name of the group; may not be null
     * @param clientId the name of the client; may not be null
     * @param topicName the name of the topic to read; may not be null and may not be empty
     * @param completion the function to call when the consumer terminates; may be null
     * @return the running interactive consumer; never null
     */
    public static KafkaCluster.InteractiveConsumer<String, Document> createConsumer(String groupId, String clientId, String topicName,
                                                                                    Runnable completion, StrimziKafkaCluster kafkaCluster) {
        Set<String> topicNames = Collections.singleton(topicName);
        return createConsumer(groupId, clientId, topicNames, new StringDeserializer(), new DocumentSerdes(), completion, kafkaCluster);
    }

    /**
     * Create an {@link KafkaCluster.InteractiveConsumer simple consumer} that can be used to read messages from the cluster.
     *
     * @param groupId the name of the group; may not be null
     * @param clientId the name of the client; may not be null
     * @param topicNames the names of the topics to read; may not be null and may not be empty
     * @param completion the function to call when the consumer terminates; may be null
     * @return the running interactive consumer; never null
     */
    public static KafkaCluster.InteractiveConsumer<String, Document> createConsumer(String groupId, String clientId, Set<String> topicNames,
                                                                                    Runnable completion, StrimziKafkaCluster kafkaCluster) {
        return createConsumer(groupId, clientId, topicNames, new StringDeserializer(), new DocumentSerdes(), completion, kafkaCluster);
    }

    /**
     * Use the supplied function to asynchronously consume messages from the cluster.
     *
     * @param groupId the name of the group; may not be null
     * @param clientId the name of the client; may not be null
     * @param autoOffsetReset how to pick a starting offset when there is no initial offset or if an offset is
     *            out of range; may be null for the default to be used
     * @param keyDeserializer the deserializer for the keys; may not be null
     * @param valueDeserializer the deserializer for the values; may not be null
     * @param continuation the function that determines if the consumer should continue; may not be null
     * @param offsetCommitCallback the callback that should be used after committing offsets; may be null if offsets are
     *            not to be committed
     * @param completion the function to call when the consumer terminates; may be null
     * @param topics the set of topics to consume; may not be null or empty
     * @param consumerFunction the function to consume the messages; may not be null
     */
    public static <K, V> void consume(String groupId, String clientId, OffsetResetStrategy autoOffsetReset,
                                      Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
                                      BooleanSupplier continuation, OffsetCommitCallback offsetCommitCallback, Runnable completion,
                                      Collection<String> topics,
                                      Consumer<ConsumerRecord<K, V>> consumerFunction,
                                      StrimziKafkaCluster kafkaCluster) {
        Properties props = getConsumerProperties(groupId, clientId, autoOffsetReset, kafkaCluster);
        Thread t = new Thread(() -> {
            LOGGER.debug("Starting consumer {} to read messages", clientId);
            try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer)) {
                consumer.subscribe(new ArrayList<>(topics));
                while (continuation.getAsBoolean()) {
                    consumer.poll(Duration.ofMillis(10)).forEach(record -> {
                        LOGGER.debug("Consumer {}: consuming message {}", clientId, record);
                        consumerFunction.accept(record);
                        if (offsetCommitCallback != null) {
                            consumer.commitAsync(offsetCommitCallback);
                        }
                    });
                }
            }
            finally {
                if (completion != null) {
                    completion.run();
                }
                LOGGER.debug("Stopping consumer {}", clientId);
            }
        });
        t.setName(clientId + "-thread");
        t.start();
    }

    /**
     * Use the supplied function to asynchronously produce messages and write them to the cluster.
     *
     * @param producerName the name of the producer; may not be null
     * @param producer the function that will asynchronously produce messages
     */
    public static <K, V> void produce(String producerName, Consumer<KafkaCluster.InteractiveProducer<String, Document>> producer,
                                      StrimziKafkaCluster kafkaCluster) {
        produce(producerName, new StringSerializer(), new DocumentSerdes(), producer, kafkaCluster);
    }

    /**
     * Use the supplied function to asynchronously produce messages and write them to the cluster.
     *
     * @param producerName the name of the producer; may not be null
     * @param keySerializer the serializer for the keys; may not be null
     * @param valueSerializer the serializer for the values; may not be null
     * @param producer the function that will asynchronously produce messages
     */
    public static <K, V> void produce(String producerName, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                      Consumer<KafkaCluster.InteractiveProducer<K, V>> producer, StrimziKafkaCluster kafkaCluster) {
        Properties props = getProducerProperties(producerName, kafkaCluster);
        KafkaProducer<K, V> kafkaProducer = new KafkaProducer<>(props, keySerializer, valueSerializer);
        KafkaCluster.InteractiveProducer<K, V> interactive = new KafkaCluster.InteractiveProducer<K, V>() {
            @Override
            public KafkaCluster.InteractiveProducer<K, V> write(ProducerRecord<K, V> record) {
                kafkaProducer.send(record);
                kafkaProducer.flush();
                return this;
            }

            @Override
            public void close() {
                kafkaProducer.close();
            }
        };
        Thread t = new Thread(() -> {
            try {
                producer.accept(interactive);
            }
            finally {
                interactive.close();
            }
        });
        t.setName(producerName + "-thread");
        t.start();
    }

    /**
     * Asynchronously produce messages with String keys and sequential Integer values, and write them to the cluster.
     *
     * @param topic the name of the topic to which the messages should be written; may not be null
     * @param messageCount the number of messages to produce; must be positive
     * @param initialValue the first integer value to produce
     * @param completionCallback the function to be called when the producer is completed; may be null
     */
    public static void produceIntegers(String topic, int messageCount, int initialValue,
                                       Runnable completionCallback, StrimziKafkaCluster kafkaCluster) {
        AtomicLong counter = new AtomicLong(initialValue);
        produceIntegers(messageCount, completionCallback, () -> {
            long i = counter.incrementAndGet();
            String keyAndValue = Long.toString(i);
            return new ProducerRecord<String, Integer>(topic, keyAndValue, Integer.valueOf((int) i));
        }, kafkaCluster);
    }

    /**
     * Use the supplied function to asynchronously produce messages with String keys and Integer values, and write
     * them to the cluster.
     *
     * @param messageCount the number of messages to produce; must be positive
     * @param completionCallback the function to be called when the producer is completed; may be null
     * @param messageSupplier the function to produce messages; may not be null
     */
    public static void produceIntegers(int messageCount,
                                       Runnable completionCallback, Supplier<ProducerRecord<String, Integer>> messageSupplier,
                                       StrimziKafkaCluster kafkaCluster) {
        Serializer<String> keySer = new StringSerializer();
        Serializer<Integer> valSer = new IntegerSerializer();
        String randomId = UUID.randomUUID().toString();
        produce(randomId, messageCount, keySer, valSer, completionCallback, messageSupplier, kafkaCluster);
    }

    /**
     * Use the supplied function to asynchronously produce messages and write them to the cluster.
     *
     * @param producerName the name of the producer; may not be null
     * @param messageCount the number of messages to produce; must be positive
     * @param keySerializer the serializer for the keys; may not be null
     * @param valueSerializer the serializer for the values; may not be null
     * @param completionCallback the function to be called when the producer is completed; may be null
     * @param messageSupplier the function to produce messages; may not be null
     */
    public static <K, V> void produce(String producerName, int messageCount,
                                      Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                      Runnable completionCallback,
                                      Supplier<ProducerRecord<K, V>> messageSupplier, StrimziKafkaCluster kafkaCluster) {
        Properties props = getProducerProperties(producerName, kafkaCluster);
        Thread t = new Thread(() -> {
            LOGGER.debug("Starting producer {} to write {} messages", producerName, messageCount);
            try (KafkaProducer<K, V> producer = new KafkaProducer<>(props, keySerializer, valueSerializer)) {
                for (int i = 0; i != messageCount; ++i) {
                    ProducerRecord<K, V> record = messageSupplier.get();
                    producer.send(record);
                    producer.flush();
                    LOGGER.debug("Producer {}: sent message {}", producerName, record);
                }
            }
            finally {
                if (completionCallback != null) {
                    completionCallback.run();
                }
                LOGGER.debug("Stopping producer {}", producerName);
            }
        });
        t.setName(producerName + "-thread");
        t.start();
    }

    /**
     * Asynchronously consume all messages on the given topic from the cluster.
     *
     * @param topicName the name of the topic; may not be null
     * @param count the expected number of messages to read before terminating; may not be null
     * @param timeout the maximum time that this consumer should run before terminating; must be positive
     * @param unit the unit of time for the timeout; may not be null
     * @param completion the function to call when all messages have been consumed; may be null
     * @param consumer the function to consume the messages; may not be null
     */
    public static void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
                                      BiPredicate<String, String> consumer, StrimziKafkaCluster kafkaCluster) {
        AtomicLong readCounter = new AtomicLong();
        consumeStrings(continueIfNotExpired(() -> readCounter.get() < count, timeout, unit),
                completion,
                Collections.singleton(topicName),
                record -> {
                    if (consumer.test(record.key(), record.value())) {
                        readCounter.incrementAndGet();
                    }
                }, kafkaCluster);
    }

    /**
     * Asynchronously consume all messages from the cluster.
     *
     * @param continuation the function that determines if the consumer should continue; may not be null
     * @param completion the function to call when all messages have been consumed; may be null
     * @param topics the set of topics to consume; may not be null or empty
     * @param consumerFunction the function to consume the messages; may not be null
     */
    public static void consumeStrings(BooleanSupplier continuation, Runnable completion, Collection<String> topics,
                                      Consumer<ConsumerRecord<String, String>> consumerFunction, StrimziKafkaCluster kafkaCluster) {
        Deserializer<String> keyDes = new StringDeserializer();
        Deserializer<String> valDes = keyDes;
        String randomId = UUID.randomUUID().toString();
        OffsetCommitCallback offsetCommitCallback = null;
        consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, valDes, continuation, offsetCommitCallback,
                completion, topics, consumerFunction, kafkaCluster);
    }

    /**
     * Asynchronously consume all messages on the given topic from the cluster.
     *
     * @param topicName the name of the topic; may not be null
     * @param count the expected number of messages to read before terminating; may not be null
     * @param timeout the maximum time that this consumer should run before terminating; must be positive
     * @param unit the unit of time for the timeout; may not be null
     * @param completion the function to call when all messages have been consumed; may be null
     */
    public static void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
                                      StrimziKafkaCluster kafkaCluster) {
        consumeStrings(topicName, count, timeout, unit, completion, (key, value) -> true, kafkaCluster);
    }

    protected static BooleanSupplier continueIfNotExpired(BooleanSupplier continuation, long timeout, TimeUnit unit) {
        return new BooleanSupplier() {
            long stopTime = 0L;

            @Override
            public boolean getAsBoolean() {
                if (stopTime == 0L) {
                    stopTime = System.currentTimeMillis() + unit.toMillis(timeout);
                }
                return continuation.getAsBoolean() && System.currentTimeMillis() <= stopTime;
            }
        };
    }
}

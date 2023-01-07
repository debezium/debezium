/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kafka;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.document.Document;
import io.debezium.document.DocumentSerdes;
import io.debezium.util.IoUtil;

/**
 * An embeddable cluster of Kafka servers and a single Zookeeper server. This may be useful when creating a complete environment
 * within a single process, but doing so offers limited durability and fault tolerance compared to the normal approach of
 * using an external cluster of Kafka servers and Zookeeper servers with proper replication and fault tolerance.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class KafkaCluster {

    public static final boolean DEFAULT_DELETE_DATA_UPON_SHUTDOWN = true;
    public static final boolean DEFAULT_DELETE_DATA_PRIOR_TO_STARTUP = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCluster.class);

    private final ConcurrentMap<Integer, KafkaServer> kafkaServers = new ConcurrentHashMap<>();
    private final ZookeeperServer zkServer = new ZookeeperServer();
    private volatile File dataDir = null;
    private volatile boolean deleteDataUponShutdown = DEFAULT_DELETE_DATA_UPON_SHUTDOWN;
    private volatile boolean deleteDataPriorToStartup = DEFAULT_DELETE_DATA_PRIOR_TO_STARTUP;
    private volatile boolean running = false;
    private volatile Properties kafkaConfig = null;
    private volatile int startingKafkaPort = -1;
    private final AtomicLong nextKafkaPort = new AtomicLong(startingKafkaPort);

    /**
     * Create a new embedded cluster.
     */
    public KafkaCluster() {
    }

    /**
     * Specify whether the data is to be deleted upon {@link #shutdown()}.
     *
     * @param delete true if the data is to be deleted upon shutdown, or false otherwise
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the cluster is running
     */
    public KafkaCluster deleteDataUponShutdown(boolean delete) {
        if (running) {
            throw new IllegalStateException("Unable to change cluster settings when running");
        }
        this.deleteDataUponShutdown = delete;
        return this;
    }

    /**
     * Specify whether the data is to be deleted prior to {@link #startup()}.
     *
     * @param delete true if the data is to be deleted upon shutdown, or false otherwise
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the cluster is running
     */
    public KafkaCluster deleteDataPriorToStartup(boolean delete) {
        if (running) {
            throw new IllegalStateException("Unable to change cluster settings when running");
        }
        this.deleteDataPriorToStartup = delete;
        return this;
    }

    /**
     * Add a number of new Kafka broker to the cluster. The broker IDs will be generated.
     *
     * @param count the number of new brokers to add
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the cluster is running
     */
    public KafkaCluster addBrokers(int count) {
        if (running) {
            throw new IllegalStateException("Unable to add a broker when the cluster is already running");
        }
        AtomicLong added = new AtomicLong();
        while (added.intValue() < count) {
            kafkaServers.computeIfAbsent(Integer.valueOf(added.intValue() + 1), id -> {
                added.incrementAndGet();
                KafkaServer server = new KafkaServer(zkServer::getConnection, id);
                if (dataDir != null) {
                    server.setStateDirectory(dataDir);
                }
                if (kafkaConfig != null) {
                    server.setProperties(kafkaConfig);
                }
                if (startingKafkaPort >= 0) {
                    server.setPort((int) this.nextKafkaPort.getAndIncrement());
                }
                return server;
            });
        }
        return this;
    }

    /**
     * Set the parent directory where the brokers logs and server's logs and snapshots will be kept.
     *
     * @param dataDir the parent directory for the server's logs and snapshots; may be null if a temporary directory will be used
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the cluster is running
     * @throws IllegalArgumentException if the supplied file is not a directory or not writable
     */
    public KafkaCluster usingDirectory(File dataDir) {
        if (running) {
            throw new IllegalStateException("Unable to add a broker when the cluster is already running");
        }
        if (dataDir != null && dataDir.exists() && !dataDir.isDirectory() && !dataDir.canWrite() && !dataDir.canRead()) {
            throw new IllegalArgumentException("The directory must be readable and writable");
        }
        this.dataDir = dataDir;
        return this;
    }

    /**
     * Set the configuration properties for each of the brokers. This method does nothing if the supplied properties are null or
     * empty.
     *
     * @param properties the Kafka configuration properties
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the cluster is running
     */
    public KafkaCluster withKafkaConfiguration(Properties properties) {
        if (running) {
            throw new IllegalStateException("Unable to add a broker when the cluster is already running");
        }
        if (properties != null && !properties.isEmpty()) {
            kafkaConfig = new Properties();
            kafkaConfig.putAll(properties);
            kafkaServers.values().forEach(kafka -> kafka.setProperties(kafkaConfig));
        }
        return this;
    }

    /**
     * Set the port numbers for Zookeeper and the Kafka brokers.
     *
     * @param zkPort the port number that Zookeeper should use; may be -1 if an available port should be discovered
     * @param firstKafkaPort the port number for the first Kafka broker (additional brokers will use subsequent port numbers);
     *            may be -1 if available ports should be discovered
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the cluster is running
     */
    public KafkaCluster withPorts(int zkPort, int firstKafkaPort) {
        if (running) {
            throw new IllegalStateException("Unable to add a broker when the cluster is already running");
        }
        this.zkServer.setPort(zkPort);
        this.startingKafkaPort = firstKafkaPort;
        if (this.startingKafkaPort >= 0) {
            this.nextKafkaPort.set(this.startingKafkaPort);
            kafkaServers.values().forEach(kafka -> kafka.setPort((int) this.nextKafkaPort.getAndIncrement()));
        }
        return this;
    }

    /**
     * Determine if the cluster is running.
     *
     * @return true if the cluster is running, or false otherwise
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Start the embedded Zookeeper server and the Kafka servers {@link #addBrokers(int) in the cluster}.
     * This method does nothing if the cluster is already running.
     *
     * @return this instance to allow chaining methods; never null
     * @throws IOException if there is an error during startup
     */
    public synchronized KafkaCluster startup() throws IOException {
        if (!running) {
            if (dataDir == null) {
                try {
                    File temp = File.createTempFile("kafka", "suffix");
                    dataDir = new File(temp.getParentFile(), "cluster");
                    dataDir.mkdirs();
                    temp.delete();
                }
                catch (IOException e) {
                    throw new RuntimeException("Unable to create temporary directory", e);
                }
            }
            else if (deleteDataPriorToStartup) {
                IoUtil.delete(dataDir);
                dataDir.mkdirs();
            }
            File zkDir = new File(dataDir, "zk");
            zkServer.setStateDirectory(zkDir); // does error checking
            this.dataDir = dataDir;
            File kafkaDir = new File(dataDir, "kafka");
            kafkaServers.values().forEach(server -> server.setStateDirectory(new File(kafkaDir, "broker" + server.brokerId())));

            zkServer.startup();
            LOGGER.debug("Starting {} brokers", kafkaServers.size());
            kafkaServers.values().forEach(KafkaServer::startup);
            running = true;
        }
        return this;
    }

    /**
     * Shutdown the embedded Zookeeper server and the Kafka servers {@link #addBrokers(int) in the cluster}.
     * This method does nothing if the cluster is not running.
     *
     * @return this instance to allow chaining methods; never null
     */
    public synchronized KafkaCluster shutdown() {
        if (running) {
            try {
                kafkaServers.values().forEach(this::shutdownReliably);
            }
            finally {
                try {
                    zkServer.shutdown(deleteDataUponShutdown);
                }
                catch (Throwable t) {
                    LOGGER.error("Error while shutting down {}", zkServer, t);
                }
                finally {
                    if (deleteDataUponShutdown) {
                        try {
                            kafkaServers.values().forEach(KafkaServer::deleteData);
                        }
                        finally {
                            try {
                                IoUtil.delete(this.dataDir);
                            }
                            catch (IOException e) {
                                LOGGER.error("Error while deleting cluster data", e);
                            }
                        }
                    }
                    running = false;
                }
            }
        }
        return this;
    }

    /**
     * Create the specified topics.
     *
     * @param topics the names of the topics to create
     * @throws IllegalStateException if the cluster is not running
     */
    public void createTopics(String... topics) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Creating topics: {}", Arrays.toString(topics));
        }
        if (!running) {
            throw new IllegalStateException("The cluster must be running to create topics");
        }
        kafkaServers.values().stream().findFirst().ifPresent(server -> server.createTopics(topics));
    }

    /**
     * Create the specified topics.
     *
     * @param topics the names of the topics to create
     * @throws IllegalStateException if the cluster is not running
     */
    public void createTopics(Set<String> topics) {
        createTopics(topics.toArray(new String[topics.size()]));
    }

    /**
     * Create the specified topics.
     *
     * @param numPartitions the number of partitions for each topic
     * @param replicationFactor the replication factor for each topic
     * @param topics the names of the topics to create
     */
    public void createTopics(int numPartitions, int replicationFactor, String... topics) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Creating topics with {} partitions and {} replicas each: {}", numPartitions, replicationFactor,
                    Arrays.toString(topics));
        }
        if (!running) {
            throw new IllegalStateException("The cluster must be running to create topics");
        }
        kafkaServers.values().stream().findFirst().ifPresent(server -> server.createTopics(numPartitions, replicationFactor, topics));
    }

    /**
     * Create the specified topics.
     *
     * @param numPartitions the number of partitions for each topic
     * @param replicationFactor the replication factor for each topic
     * @param topics the names of the topics to create
     */
    public void createTopics(int numPartitions, int replicationFactor, Set<String> topics) {
        createTopics(numPartitions, replicationFactor, topics.toArray(new String[topics.size()]));
    }

    /**
     * Create the specified topic.
     *
     * @param topic the name of the topic to create
     * @param numPartitions the number of partitions for the topic
     * @param replicationFactor the replication factor for the topic
     */
    public void createTopic(String topic, int numPartitions, int replicationFactor) {
        LOGGER.debug("Creating topic '{}' with {} partitions and {} replicas", topic, numPartitions, replicationFactor);
        if (!running) {
            throw new IllegalStateException("The cluster must be running to create topics");
        }
        kafkaServers.values().stream().findFirst().ifPresent(server -> server.createTopic(topic, numPartitions, replicationFactor));
    }

    /**
     * Perform the supplied function on each directory used by this cluster.
     *
     * @param consumer the consumer function; may not be null
     */
    void onEachDirectory(java.util.function.Consumer<File> consumer) {
        consumer.accept(zkServer.getSnapshotDirectory());
        consumer.accept(zkServer.getLogDirectory());
        kafkaServers.values().forEach(server -> consumer.accept(server.getStateDirectory()));
    }

    /**
     * Get the list of brokers.
     *
     * @return the broker list
     */
    public String brokerList() {
        StringJoiner joiner = new StringJoiner(",");
        kafkaServers.values().forEach(server -> {
            joiner.add(server.getConnection());
        });
        return joiner.toString();
    }

    /**
     * Get the Zookeeper port.
     *
     * @return the Zookeeper port
     */
    public int zkPort() {
        return zkServer.getPort();
    }

    private void shutdownReliably(KafkaServer server) {
        try {
            server.shutdown(deleteDataUponShutdown);
        }
        catch (Throwable t) {
            LOGGER.error("Error while shutting down {}", server, t);
        }
    }

    /**
     * Obtain the interface for using this cluster.
     *
     * @return the usage interface; never null
     * @throws IllegalStateException if the cluster is not running
     */
    public Usage useTo() {
        if (!running) {
            throw new IllegalStateException("Unable to use the cluster it is not running");
        }
        return new Usage();
    }

    /**
     * A simple interactive Kafka producer for use with the cluster.
     *
     * @param <K> the type of key
     * @param <V> the type of value
     */
    public static interface InteractiveProducer<K, V> extends Closeable {
        /**
         * Write to the topic with the given name a record with the specified key and value. The message is flushed immediately.
         *
         * @param topic the name of the topic; may not be null
         * @param key the key; may not be null
         * @param value the value; may not be null
         * @return this producer instance to allow chaining methods together
         */
        default InteractiveProducer<K, V> write(String topic, K key, V value) {
            return write(new ProducerRecord<>(topic, key, value));
        }

        /**
         * Write the specified record to the topic with the given name. The message is flushed immediately.
         *
         * @param record the record; may not be null
         * @return this producer instance to allow chaining methods together
         */
        InteractiveProducer<K, V> write(ProducerRecord<K, V> record);

        /**
         * Close this producer's connection to Kafka and clean up all resources.
         */
        @Override
        public void close();
    }

    /**
     * A simple interactive Kafka consumer for use with the cluster.
     *
     * @param <K> the type of key
     * @param <V> the type of value
     */
    public interface InteractiveConsumer<K, V> extends Closeable {
        /**
         * Block until a record can be read from this consumer's topic, and return the value in that record.
         *
         * @return the value; never null
         * @throws InterruptedException if the thread is interrupted while blocking
         */
        default V nextValue() throws InterruptedException {
            return nextRecord().value();
        }

        /**
         * Block until a record can be read from this consumer's topic, and return the record.
         *
         * @return the record; never null
         * @throws InterruptedException if the thread is interrupted while blocking
         */
        ConsumerRecord<K, V> nextRecord() throws InterruptedException;

        /**
         * Block until a record can be read from this consumer's topic or until the timeout occurs, and if a record was read
         * return the value in that record.
         *
         * @param timeout the maximum amount of time to block to wait for a record
         * @param unit the unit of time for the {@code timeout}
         * @return the value, or null if the method timed out
         * @throws InterruptedException if the thread is interrupted while blocking
         */
        default V nextValue(long timeout, TimeUnit unit) throws InterruptedException {
            ConsumerRecord<K, V> record = nextRecord(timeout, unit);
            return record != null ? record.value() : null;
        }

        /**
         * Block until a record can be read from this consumer's topic or until the timeout occurs, and if a record was read
         * return the record.
         *
         * @param timeout the maximum amount of time to block to wait for a record
         * @param unit the unit of time for the {@code timeout}
         * @return the record, or null if the method timed out
         * @throws InterruptedException if the thread is interrupted while blocking
         */
        ConsumerRecord<K, V> nextRecord(long timeout, TimeUnit unit) throws InterruptedException;

        /**
         * Obtain a stream to consume the input messages. This method can be used in place of repeated calls to the
         * {@code next...()} methods.
         *
         * @return the stream of all messages.
         */
        Stream<ConsumerRecord<K, V>> stream();

        /**
         * Obtain a stream over all of accumulated input messages. This method can be called multiple times, and each time
         * the resulting stream will operate over <em>all messages</em> that received by this consumer, and is completely
         * independent of the {@link #stream()}, {@link #nextRecord()}, {@link #nextRecord(long, TimeUnit)}, {@link #nextValue()}
         * and {@link #nextValue(long, TimeUnit)} methods.
         *
         * @return the stream of all messages.
         */
        Stream<ConsumerRecord<K, V>> streamAll();

        /**
         * Asynchronously close this consumer's connection to Kafka and begin to clean up all resources.
         */
        @Override
        public void close();
    }

    /**
     * A set of methods to use a running KafkaCluster.
     */
    public class Usage {

        /**
         * Get a new set of properties for consumers that want to talk to this server.
         *
         * @param groupId the group ID for the consumer; may not be null
         * @param clientId the optional identifier for the client; may be null if not needed
         * @param autoOffsetReset how to pick a starting offset when there is no initial offset in ZooKeeper or if an offset is
         *            out of range; may be null for the default to be used
         * @return the mutable consumer properties
         * @see #getProducerProperties(String)
         */
        public Properties getConsumerProperties(String groupId, String clientId, OffsetResetStrategy autoOffsetReset) {
            if (groupId == null) {
                throw new IllegalArgumentException("The groupId is required");
            }
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());
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
         * @see #getConsumerProperties(String, String, OffsetResetStrategy)
         */
        public Properties getProducerProperties(String clientId) {
            Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList());
            props.setProperty(ProducerConfig.ACKS_CONFIG, Integer.toString(1));
            if (clientId != null) {
                props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            }
            return props;
        }

        /**
         * Create an {@link InteractiveProducer simple producer} that can be used to write messages to the cluster.
         *
         * @param producerName the name of the producer; may not be null
         * @param keySerializer the serializer for the keys; may not be null
         * @param valueSerializer the serializer for the values; may not be null
         * @return the object that can be used to produce messages; never null
         */
        public <K, V> InteractiveProducer<K, V> createProducer(String producerName, Serializer<K> keySerializer,
                                                               Serializer<V> valueSerializer) {
            Properties props = getProducerProperties(producerName);
            KafkaProducer<K, V> producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
            return new InteractiveProducer<K, V>() {
                @Override
                public InteractiveProducer<K, V> write(ProducerRecord<K, V> record) {
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
         * Create an {@link InteractiveProducer simple producer} that can be used to write {@link Document} messages to the
         * cluster.
         *
         * @param producerName the name of the producer; may not be null
         * @return the object that can be used to produce messages; never null
         */
        public InteractiveProducer<String, Document> createProducer(String producerName) {
            return createProducer(producerName, new StringSerializer(), new DocumentSerdes());
        }

        /**
         * Create an {@link InteractiveConsumer simple consumer} that can be used to read messages from the cluster.
         *
         * @param groupId the name of the group; may not be null
         * @param clientId the name of the client; may not be null
         * @param topicName the name of the topic to read; may not be null and may not be empty
         * @param keyDeserializer the deserializer for the keys; may not be null
         * @param valueDeserializer the deserializer for the values; may not be null
         * @param completion the function to call when the consumer terminates; may be null
         * @return the running interactive consumer; never null
         */
        public <K, V> InteractiveConsumer<K, V> createConsumer(String groupId, String clientId, String topicName,
                                                               Deserializer<K> keyDeserializer,
                                                               Deserializer<V> valueDeserializer, Runnable completion) {
            Set<String> topicNames = Collections.singleton(topicName);
            return createConsumer(groupId, clientId, topicNames, keyDeserializer, valueDeserializer, completion);
        }

        /**
         * Create an {@link InteractiveConsumer simple consumer} that can be used to read messages from the cluster.
         *
         * @param groupId the name of the group; may not be null
         * @param clientId the name of the client; may not be null
         * @param topicNames the names of the topics to read; may not be null and may not be empty
         * @param keyDeserializer the deserializer for the keys; may not be null
         * @param valueDeserializer the deserializer for the values; may not be null
         * @param completion the function to call when the consumer terminates; may be null
         * @return the running interactive consumer; never null
         */
        public <K, V> InteractiveConsumer<K, V> createConsumer(String groupId, String clientId, Set<String> topicNames,
                                                               Deserializer<K> keyDeserializer,
                                                               Deserializer<V> valueDeserializer, Runnable completion) {
            BlockingQueue<ConsumerRecord<K, V>> consumed = new LinkedBlockingQueue<>();
            List<ConsumerRecord<K, V>> allMessages = new LinkedList<>();
            AtomicBoolean keepReading = new AtomicBoolean();
            OffsetCommitCallback offsetCommitCallback = null;
            consume(groupId, clientId, OffsetResetStrategy.EARLIEST, keyDeserializer, valueDeserializer, () -> keepReading.get(),
                    offsetCommitCallback, completion, topicNames, record -> {
                        consumed.add(record);
                        allMessages.add(record);
                    });
            return new InteractiveConsumer<K, V>() {
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
         * Create an {@link InteractiveConsumer simple consumer} that can be used to read messages from the cluster.
         *
         * @param groupId the name of the group; may not be null
         * @param clientId the name of the client; may not be null
         * @param topicName the name of the topic to read; may not be null and may not be empty
         * @param completion the function to call when the consumer terminates; may be null
         * @return the running interactive consumer; never null
         */
        public InteractiveConsumer<String, Document> createConsumer(String groupId, String clientId, String topicName,
                                                                    Runnable completion) {
            Set<String> topicNames = Collections.singleton(topicName);
            return createConsumer(groupId, clientId, topicNames, new StringDeserializer(), new DocumentSerdes(), completion);
        }

        /**
         * Create an {@link InteractiveConsumer simple consumer} that can be used to read messages from the cluster.
         *
         * @param groupId the name of the group; may not be null
         * @param clientId the name of the client; may not be null
         * @param topicNames the names of the topics to read; may not be null and may not be empty
         * @param completion the function to call when the consumer terminates; may be null
         * @return the running interactive consumer; never null
         */
        public InteractiveConsumer<String, Document> createConsumer(String groupId, String clientId, Set<String> topicNames,
                                                                    Runnable completion) {
            return createConsumer(groupId, clientId, topicNames, new StringDeserializer(), new DocumentSerdes(), completion);
        }

        /**
         * Use the supplied function to asynchronously produce {@link Document} messages and write them to the cluster.
         *
         * @param producerName the name of the producer; may not be null
         * @param producer the function that will asynchronously
         */
        public <K, V> void produce(String producerName, Consumer<InteractiveProducer<String, Document>> producer) {
            produce(producerName, new StringSerializer(), new DocumentSerdes(), producer);
        }

        /**
         * Use the supplied function to asynchronously produce messages and write them to the cluster.
         *
         * @param producerName the name of the producer; may not be null
         * @param keySerializer the serializer for the keys; may not be null
         * @param valueSerializer the serializer for the values; may not be null
         * @param producer the function that will asynchronously
         */
        public <K, V> void produce(String producerName, Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                   Consumer<InteractiveProducer<K, V>> producer) {
            Properties props = getProducerProperties(producerName);
            KafkaProducer<K, V> kafkaProducer = new KafkaProducer<>(props, keySerializer, valueSerializer);
            InteractiveProducer<K, V> interactive = new InteractiveProducer<K, V>() {
                @Override
                public InteractiveProducer<K, V> write(ProducerRecord<K, V> record) {
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
         * Use the supplied function to asynchronously produce messages and write them to the cluster.
         *
         * @param producerName the name of the producer; may not be null
         * @param messageCount the number of messages to produce; must be positive
         * @param keySerializer the serializer for the keys; may not be null
         * @param valueSerializer the serializer for the values; may not be null
         * @param completionCallback the function to be called when the producer is completed; may be null
         * @param messageSupplier the function to produce messages; may not be null
         */
        public <K, V> void produce(String producerName, int messageCount,
                                   Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                   Runnable completionCallback,
                                   Supplier<ProducerRecord<K, V>> messageSupplier) {
            Properties props = getProducerProperties(producerName);
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
         * Use the supplied function to asynchronously produce messages with String keys and values, and write them to the
         * cluster.
         *
         * @param messageCount the number of messages to produce; must be positive
         * @param completionCallback the function to be called when the producer is completed; may be null
         * @param messageSupplier the function to produce messages; may not be null
         */
        public void produceStrings(int messageCount,
                                   Runnable completionCallback, Supplier<ProducerRecord<String, String>> messageSupplier) {
            Serializer<String> keySer = new StringSerializer();
            Serializer<String> valSer = keySer;
            String randomId = UUID.randomUUID().toString();
            produce(randomId, messageCount, keySer, valSer, completionCallback, messageSupplier);
        }

        /**
         * Use the supplied function to asynchronously produce messages with String keys and {@link Document} values, and write
         * them to the cluster.
         *
         * @param messageCount the number of messages to produce; must be positive
         * @param completionCallback the function to be called when the producer is completed; may be null
         * @param messageSupplier the function to produce messages; may not be null
         */
        public void produceDocuments(int messageCount,
                                     Runnable completionCallback, Supplier<ProducerRecord<String, Document>> messageSupplier) {
            Serializer<String> keySer = new StringSerializer();
            Serializer<Document> valSer = new DocumentSerdes();
            String randomId = UUID.randomUUID().toString();
            produce(randomId, messageCount, keySer, valSer, completionCallback, messageSupplier);
        }

        /**
         * Use the supplied function to asynchronously produce messages with String keys and Integer values, and write them to the
         * cluster.
         *
         * @param messageCount the number of messages to produce; must be positive
         * @param completionCallback the function to be called when the producer is completed; may be null
         * @param messageSupplier the function to produce messages; may not be null
         */
        public void produceIntegers(int messageCount,
                                    Runnable completionCallback, Supplier<ProducerRecord<String, Integer>> messageSupplier) {
            Serializer<String> keySer = new StringSerializer();
            Serializer<Integer> valSer = new IntegerSerializer();
            String randomId = UUID.randomUUID().toString();
            produce(randomId, messageCount, keySer, valSer, completionCallback, messageSupplier);
        }

        /**
         * Asynchronously produce messages with String keys and sequential Integer values, and write them to the cluster.
         *
         * @param topic the name of the topic to which the messages should be written; may not be null
         * @param messageCount the number of messages to produce; must be positive
         * @param initialValue the first integer value to produce
         * @param completionCallback the function to be called when the producer is completed; may be null
         */
        public void produceIntegers(String topic, int messageCount, int initialValue,
                                    Runnable completionCallback) {
            AtomicLong counter = new AtomicLong(initialValue);
            produceIntegers(messageCount, completionCallback, () -> {
                long i = counter.incrementAndGet();
                String keyAndValue = Long.toString(i);
                return new ProducerRecord<String, Integer>(topic, keyAndValue, Integer.valueOf((int) i));
            });
        }

        /**
         * Asynchronously produce messages with monotonically increasing String keys and values obtained from the supplied
         * function, and write them to the cluster.
         *
         * @param topic the name of the topic to which the messages should be written; may not be null
         * @param messageCount the number of messages to produce; must be positive
         * @param completionCallback the function to be called when the producer is completed; may be null
         * @param valueSupplier the value supplier; may not be null
         */
        public void produceStrings(String topic, int messageCount,
                                   Runnable completionCallback, Supplier<String> valueSupplier) {
            AtomicLong counter = new AtomicLong(0);
            produceStrings(messageCount, completionCallback, () -> {
                long i = counter.incrementAndGet();
                String keyAndValue = Long.toString(i);
                return new ProducerRecord<String, String>(topic, keyAndValue, valueSupplier.get());
            });
        }

        /**
         * Asynchronously produce messages with monotonically increasing String keys and values obtained from the supplied
         * function, and write them to the cluster.
         *
         * @param topic the name of the topic to which the messages should be written; may not be null
         * @param messageCount the number of messages to produce; must be positive
         * @param completionCallback the function to be called when the producer is completed; may be null
         * @param valueSupplier the value supplier; may not be null
         */
        public void produceDocuments(String topic, int messageCount,
                                     Runnable completionCallback, Supplier<Document> valueSupplier) {
            AtomicLong counter = new AtomicLong(0);
            produceDocuments(messageCount, completionCallback, () -> {
                long i = counter.incrementAndGet();
                String keyAndValue = Long.toString(i);
                return new ProducerRecord<String, Document>(topic, keyAndValue, valueSupplier.get());
            });
        }

        /**
         * Use the supplied function to asynchronously consume messages from the cluster.
         *
         * @param groupId the name of the group; may not be null
         * @param clientId the name of the client; may not be null
         * @param autoOffsetReset how to pick a starting offset when there is no initial offset in ZooKeeper or if an offset is
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
        public <K, V> void consume(String groupId, String clientId, OffsetResetStrategy autoOffsetReset,
                                   Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
                                   BooleanSupplier continuation, OffsetCommitCallback offsetCommitCallback, Runnable completion,
                                   Collection<String> topics,
                                   java.util.function.Consumer<ConsumerRecord<K, V>> consumerFunction) {
            Properties props = getConsumerProperties(groupId, clientId, autoOffsetReset);
            Thread t = new Thread(() -> {
                LOGGER.debug("Starting consumer {} to read messages", clientId);
                try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer)) {
                    consumer.subscribe(new ArrayList<>(topics));
                    while (continuation.getAsBoolean()) {
                        consumer.poll(10).forEach(record -> {
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
         * Asynchronously consume all messages from the cluster.
         *
         * @param continuation the function that determines if the consumer should continue; may not be null
         * @param completion the function to call when all messages have been consumed; may be null
         * @param topics the set of topics to consume; may not be null or empty
         * @param consumerFunction the function to consume the messages; may not be null
         */
        public void consumeDocuments(BooleanSupplier continuation, Runnable completion, Collection<String> topics,
                                     java.util.function.Consumer<ConsumerRecord<String, Document>> consumerFunction) {
            Deserializer<String> keyDes = new StringDeserializer();
            Deserializer<Document> valDes = new DocumentSerdes();
            String randomId = UUID.randomUUID().toString();
            OffsetCommitCallback offsetCommitCallback = null;
            consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, valDes, continuation, offsetCommitCallback,
                    completion, topics, consumerFunction);
        }

        /**
         * Asynchronously consume all messages from the cluster.
         *
         * @param continuation the function that determines if the consumer should continue; may not be null
         * @param completion the function to call when all messages have been consumed; may be null
         * @param topics the set of topics to consume; may not be null or empty
         * @param consumerFunction the function to consume the messages; may not be null
         */
        public void consumeStrings(BooleanSupplier continuation, Runnable completion, Collection<String> topics,
                                   java.util.function.Consumer<ConsumerRecord<String, String>> consumerFunction) {
            Deserializer<String> keyDes = new StringDeserializer();
            Deserializer<String> valDes = keyDes;
            String randomId = UUID.randomUUID().toString();
            OffsetCommitCallback offsetCommitCallback = null;
            consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, valDes, continuation, offsetCommitCallback,
                    completion, topics, consumerFunction);
        }

        /**
         * Asynchronously consume all messages from the cluster.
         *
         * @param continuation the function that determines if the consumer should continue; may not be null
         * @param completion the function to call when all messages have been consumed; may be null
         * @param topics the set of topics to consume; may not be null or empty
         * @param consumerFunction the function to consume the messages; may not be null
         */
        public void consumeIntegers(BooleanSupplier continuation, Runnable completion, Collection<String> topics,
                                    java.util.function.Consumer<ConsumerRecord<String, Integer>> consumerFunction) {
            Deserializer<String> keyDes = new StringDeserializer();
            Deserializer<Integer> valDes = new IntegerDeserializer();
            String randomId = UUID.randomUUID().toString();
            OffsetCommitCallback offsetCommitCallback = null;
            consume(randomId, randomId, OffsetResetStrategy.EARLIEST, keyDes, valDes, continuation, offsetCommitCallback,
                    completion, topics, consumerFunction);
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
        public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
                                   BiPredicate<String, String> consumer) {
            AtomicLong readCounter = new AtomicLong();
            consumeStrings(continueIfNotExpired(() -> readCounter.get() < count, timeout, unit),
                    completion,
                    Collections.singleton(topicName),
                    record -> {
                        if (consumer.test(record.key(), record.value())) {
                            readCounter.incrementAndGet();
                        }
                    });
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
        public void consumeDocuments(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
                                     BiPredicate<String, Document> consumer) {
            AtomicLong readCounter = new AtomicLong();
            consumeDocuments(continueIfNotExpired(() -> readCounter.get() < count, timeout, unit),
                    completion,
                    Collections.singleton(topicName),
                    record -> {
                        if (consumer.test(record.key(), record.value())) {
                            readCounter.incrementAndGet();
                        }
                    });
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
        public void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Runnable completion,
                                    BiPredicate<String, Integer> consumer) {
            AtomicLong readCounter = new AtomicLong();
            consumeIntegers(continueIfNotExpired(() -> readCounter.get() < count, timeout, unit),
                    completion,
                    Collections.singleton(topicName),
                    record -> {
                        if (consumer.test(record.key(), record.value())) {
                            readCounter.incrementAndGet();
                        }
                    });
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
        public void consumeStrings(String topicName, int count, long timeout, TimeUnit unit, Runnable completion) {
            consumeStrings(topicName, count, timeout, unit, completion, (key, value) -> true);
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
        public void consumeDocuments(String topicName, int count, long timeout, TimeUnit unit, Runnable completion) {
            consumeDocuments(topicName, count, timeout, unit, completion, (key, value) -> true);
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
        public void consumeIntegers(String topicName, int count, long timeout, TimeUnit unit, Runnable completion) {
            consumeIntegers(topicName, count, timeout, unit, completion, (key, value) -> true);
        }

        protected BooleanSupplier continueIfNotExpired(BooleanSupplier continuation, long timeout, TimeUnit unit) {
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
}

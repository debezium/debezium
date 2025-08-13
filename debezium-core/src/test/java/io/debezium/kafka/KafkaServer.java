/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.util.IoUtil;

import kafka.server.KafkaConfig;

/**
 * A small embedded Kafka server.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class KafkaServer {

    public static final int DEFAULT_BROKER_ID = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServer.class);

    private final int brokerId;
    private volatile File logsDir;
    private final Properties config;
    private volatile int desiredPort = -1;
    private volatile int port = -1;
    private volatile kafka.server.KafkaRaftServer server;

    /**
     * Create a new server instance.
     */
    public KafkaServer() {
        this(DEFAULT_BROKER_ID);
    }

    /**
     * Create a new server instance.
     *
     * @param brokerId the unique broker ID
     */
    public KafkaServer(int brokerId) {
        this(brokerId, -1);
    }

    /**
     * Create a new server instance.
     *
     * @param brokerId the unique broker ID
     * @param port the desired port
     */
    public KafkaServer(int brokerId, int port) {
        this.brokerId = brokerId;
        this.config = new Properties();
        setPort(port);
        populateDefaultConfiguration(this.config);
    }

    protected int brokerId() {
        return brokerId;
    }

    /**
     * Set the initial default configuration properties. This method is called from the constructors and can be overridden
     * to customize these properties.
     *
     * @param props the configuration properties; never null
     */
    protected void populateDefaultConfiguration(Properties props) {
        config.setProperty(ServerLogConfigs.NUM_PARTITIONS_CONFIG, String.valueOf(1));
        config.setProperty(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, String.valueOf(Long.MAX_VALUE));
    }

    /**
     * Set a configuration property. Several key properties that deal with Zookeeper, and the broker ID,
     * may not be set via this method and are ignored since they are controlled elsewhere in this instance.
     *
     * @param name the property name; may not be null
     * @param value the property value; may be null
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the server is running when this method is called
     */
    public KafkaServer setProperty(String name, String value) {
        if (server != null) {
            throw new IllegalStateException("Unable to change the properties when already running");
        }
        if (!ServerConfigs.BROKER_ID_CONFIG.equalsIgnoreCase(name)
                && !ServerConfigs.BROKER_ID_CONFIG.equalsIgnoreCase(name)) {
            this.config.setProperty(name, value);
        }
        return this;
    }

    /**
     * Set multiple configuration properties. Several key properties that deal with Zookeeper, the host name, and the broker ID,
     * may not be set via this method and are ignored since they are controlled elsewhere in this instance.
     *
     * @param properties the configuration properties; may be null or empty
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the server is running when this method is called
     */
    public KafkaServer setProperties(Properties properties) {
        if (server != null) {
            throw new IllegalStateException("Unable to change the properties when already running");
        }
        properties.stringPropertyNames().forEach(propName -> {
            setProperty(propName, properties.getProperty(propName));
        });
        return this;
    }

    /**
     * Set the port for the server.
     *
     * @param port the desired port, or {@code -1} if a random available port should be found and used
     * @return this instance to allow chaining methods; never null
     */
    public KafkaServer setPort(int port) {
        this.desiredPort = port > 0 ? port : -1;
        this.port = desiredPort;
        return this;
    }

    /**
     * Get a copy of the complete configuration that is or will be used by the running server.
     *
     * @return the properties for the currently-running server; may be empty if not running
     */
    public Properties config() {
        Properties runningConfig = new Properties();
        runningConfig.putAll(config);
        runningConfig.setProperty(ServerConfigs.BROKER_ID_CONFIG, Integer.toString(brokerId));
        runningConfig.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller");
        runningConfig.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER");
        runningConfig.setProperty(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG,
                String.valueOf(config.getOrDefault(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, Boolean.TRUE)));
        // 1 partition for the __consumer_offsets_ topic should be enough
        runningConfig.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, Integer.toString(1));
        // Disable delay during every re-balance
        runningConfig.setProperty(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, Integer.toString(0));
        return runningConfig;
    }

    /**
     * Get the connection string. If the server is not {@link #startup() running} and the port is to be dynamically discovered
     * upon startup, then this method returns "{@code localhost:-1}".
     *
     * @return the connection string; never null
     */
    public String getConnection() {
        return "localhost:" + port;
    }

    /**
     * Start the embedded Kafka server.
     *
     * @return this instance to allow chaining methods; never null
     * @throws IllegalStateException if the server is already running
     */
    public synchronized KafkaServer startup() {
        if (server != null) {
            throw new IllegalStateException("" + this + " is already running");
        }

        // Determine the storage directory and adjust the configuration ...
        Properties config = config();
        if (logsDir == null) {
            try {
                File temp = File.createTempFile("kafka", "suffix");
                this.logsDir = temp.getParentFile();
                temp.delete();
            }
            catch (IOException e) {
                throw new RuntimeException("Unable to create temporary directory", e);
            }
        }
        config.setProperty(ServerLogConfigs.LOG_DIR_CONFIG, logsDir.getAbsolutePath());
        config.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, String.valueOf(1));

        // Determine the port and adjust the configuration ...
        port = desiredPort > 0 ? desiredPort : IoUtil.getAvailablePort();
        final var contollerPort = desiredPort > 0 ? desiredPort + 1 : IoUtil.getAvailablePort();
        config.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://localhost:" + port);
        config.setProperty(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://:%s,CONTROLLER://:%s".formatted(port, contollerPort));
        config.setProperty(QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG, getConnection());
        LOGGER.debug("Using Kafka Server configuration: {}", config);

        // Start the server ...
        try {
            LOGGER.debug("Starting Kafka broker {} at {} with storage in {}", brokerId, getConnection(), logsDir.getAbsolutePath());
            final var kafkaConfig = new KafkaConfig(config);

            final var formatter = new KafkaStorageFormatter(kafkaConfig);
            formatter.format();

            server = new kafka.server.KafkaRaftServer(kafkaConfig, Time.SYSTEM);
            server.startup();
            LOGGER.info("Started Kafka server {} at {} with storage in {}", brokerId, getConnection(), logsDir.getAbsolutePath());
            return this;
        }
        catch (RuntimeException e) {
            server = null;
            throw e;
        }
    }

    /**
     * Shutdown the embedded Kafka server and delete all data.
     *
     * @param deleteLogs whether or not to remove all the log files after shutting down
     */
    public synchronized void shutdown(boolean deleteLogs) {
        if (server != null) {
            try {
                server.shutdown();
                if (deleteLogs) {
                    // Kafka 4.x does not expose a method to delete the logs
                    // At least on Linux it seems there are no open handles to the log files
                    // so we don't delete them here.
                }
                LOGGER.info("Stopped Kafka server {} at {}", brokerId, getConnection());
            }
            finally {
                server = null;
                port = desiredPort;
            }
        }
    }

    /**
     * Delete all of the data associated with this server.
     */
    public synchronized void deleteData() {
        if (server == null) {
            // Delete all data ...
            try {
                IoUtil.delete(this.logsDir);
            }
            catch (IOException e) {
                LOGGER.error("Unable to delete directory '{}'", this.logsDir, e);
            }
        }
    }

    /**
     * Create the specified topics.
     *
     * @param topics the names of the topics to create
     */
    public void createTopics(String... topics) {
        createTopics(1, 1, topics);
    }

    /**
     * Create the specified topics.
     *
     * @param numPartitions the number of partitions for each topic
     * @param replicationFactor the replication factor for each topic
     * @param topics the names of the topics to create
     */
    public void createTopics(int numPartitions, int replicationFactor, String... topics) {
        for (String topic : topics) {
            if (topic != null) {
                createTopic(topic, numPartitions, replicationFactor);
            }
        }
    }

    /**
     * Create the specified topic.
     *
     * @param topic the name of the topic to create
     * @param numPartitions the number of partitions for the topic
     * @param replicationFactor the replication factor for the topic
     */
    public void createTopic(String topic, int numPartitions, int replicationFactor) {
        final var clientId = KafkaServer.class.getSimpleName() + "-" + brokerId + "-topic-create";
        final var adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getConnection());

        try (AdminClient admin = AdminClient.create(adminConfig)) {
            admin.createTopics(Collections.singletonList(new NewTopic(topic, numPartitions, (short) replicationFactor)))
                    .all().get();
            LOGGER.info("Created topic '{}' with {} partitions and replication factor {}", topic, numPartitions,
                    replicationFactor);
        }
        catch (Exception e) {
            throw new DebeziumException("Unable to create topic '" + topic + "'", e);
        }
    }

    /**
     * Perform the supplied function on each directory used by this server.
     *
     * @param consumer the consumer function; may not be null
     */
    void onEachDirectory(Consumer<File> consumer) {
        consumer.accept(getStateDirectory());
    }

    /**
     * Get the parent directory where the broker's state will be kept. The broker will create a subdirectory for itself
     * under this directory.
     *
     * @return the parent directory for the broker's state; may be null if a temporary directory will be used
     */
    public File getStateDirectory() {
        return this.logsDir;
    }

    /**
     * Set the parent directory where the broker's state will be kept. The broker will create a subdirectory for itself
     * under this directory.
     *
     * @param stateDirectory the parent directory for the broker's state; may be null if a temporary directory will be used
     * @throws IllegalArgumentException if the supplied file is not a directory or not writable
     */
    public void setStateDirectory(File stateDirectory) {
        if (stateDirectory != null && stateDirectory.exists() && !stateDirectory.isDirectory() && !stateDirectory.canWrite()
                && !stateDirectory.canRead()) {
            throw new IllegalArgumentException("The directory must be readable and writable");
        }
        this.logsDir = stateDirectory;
    }

    @Override
    public String toString() {
        return "KafkaServer{" + getConnection() + "}";
    }
}

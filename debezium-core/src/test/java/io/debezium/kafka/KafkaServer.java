/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.util.IoUtil;

import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.zk.AdminZkClient;
import scala.collection.JavaConverters;

/**
 * A small embedded Kafka server.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public class KafkaServer {

    public static final int DEFAULT_BROKER_ID = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServer.class);

    private final Supplier<String> zkConnection;
    private final int brokerId;
    private volatile File logsDir;
    private final Properties config;
    private volatile int desiredPort = -1;
    private volatile int port = -1;
    private volatile kafka.server.KafkaServer server;
    private volatile AdminZkClient adminZkClient;

    /**
     * Create a new server instance.
     *
     * @param zookeeperConnection the supplier of the Zookeeper connection string; may not be null
     */
    public KafkaServer(Supplier<String> zookeeperConnection) {
        this(zookeeperConnection, DEFAULT_BROKER_ID);
    }

    /**
     * Create a new server instance.
     *
     * @param zookeeperConnection the supplier of the Zookeeper connection string; may not be null
     * @param brokerId the unique broker ID
     */
    public KafkaServer(Supplier<String> zookeeperConnection, int brokerId) {
        this(zookeeperConnection, brokerId, -1);
    }

    /**
     * Create a new server instance.
     *
     * @param zookeeperConnection the supplier of the Zookeeper connection string; may not be null
     * @param brokerId the unique broker ID
     * @param port the desired port
     */
    public KafkaServer(Supplier<String> zookeeperConnection, int brokerId, int port) {
        if (zookeeperConnection == null) {
            throw new IllegalArgumentException("The Zookeeper connection string supplier may not be null");
        }
        this.zkConnection = zookeeperConnection;
        this.brokerId = brokerId;
        this.config = new Properties();
        setPort(port);
        populateDefaultConfiguration(this.config);
    }

    protected int brokerId() {
        return brokerId;
    }

    protected String zookeeperConnection() {
        return this.zkConnection.get();
    }

    /**
     * Set the initial default configuration properties. This method is called from the constructors and can be overridden
     * to customize these properties.
     *
     * @param props the configuration properties; never null
     */
    protected void populateDefaultConfiguration(Properties props) {
        config.setProperty(KafkaConfig.NumPartitionsProp(), String.valueOf(1));
        config.setProperty(KafkaConfig.LogFlushIntervalMessagesProp(), String.valueOf(Long.MAX_VALUE));
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
        if (!KafkaConfig.ZkConnectProp().equalsIgnoreCase(name)
                && !KafkaConfig.BrokerIdProp().equalsIgnoreCase(name)) {
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
        runningConfig.setProperty(KafkaConfig.ZkConnectProp(), zookeeperConnection());
        runningConfig.setProperty(KafkaConfig.BrokerIdProp(), Integer.toString(brokerId));
        runningConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp(), String.valueOf(config.getOrDefault(KafkaConfig.AutoCreateTopicsEnableProp(), Boolean.TRUE)));
        // 1 partition for the __consumer_offsets_ topic should be enough
        runningConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp(), Integer.toString(1));
        // Disable delay during every re-balance
        runningConfig.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp(), Integer.toString(0));
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
        config.setProperty(KafkaConfig.LogDirProp(), logsDir.getAbsolutePath());
        config.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), String.valueOf(1));

        // Determine the port and adjust the configuration ...
        port = desiredPort > 0 ? desiredPort : IoUtil.getAvailablePort();
        config.setProperty(KafkaConfig.ListenersProp(), "PLAINTEXT://localhost:" + port);
        // config.setProperty("metadata.broker.list", getConnection());

        // Start the server ...
        try {
            LOGGER.debug("Starting Kafka broker {} at {} with storage in {}", brokerId, getConnection(), logsDir.getAbsolutePath());
            server = new kafka.server.KafkaServer(new KafkaConfig(config), Time.SYSTEM, scala.Option.apply(null),
                    false);
            server.startup();
            LOGGER.info("Started Kafka server {} at {} with storage in {}", brokerId, getConnection(), logsDir.getAbsolutePath());
            adminZkClient = new AdminZkClient(server.zkClient());
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
                    // as of 0.10.1.1 if logs are not deleted explicitly, there are open File Handles left on .timeindex files
                    // at least on Windows courtesy of the TimeIndex.scala class
                    // NOTE: specifically do not use method reference to ensure compatibility between Kafka 3.0.x and 3.1+
                    JavaConverters.asJavaIterableConverter(server.logManager().allLogs()).asJava().forEach(l -> l.delete());
                }
                LOGGER.info("Stopped Kafka server {} at {}", brokerId, getConnection());
            }
            finally {
                server = null;
                adminZkClient = null;
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
     * Get the Zookeeper admin client used by the running Kafka server.
     *
     * @return the Zookeeper admin client, or null if the Kafka server is not running
     */
    public AdminZkClient getAdminZkClient() {
        return adminZkClient;
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
        RackAwareMode rackAwareMode = null;
        getAdminZkClient().createTopic(topic, numPartitions, replicationFactor, new Properties(), rackAwareMode, false);
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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kafka;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.util.IoUtil;

/**
 * A lightweight embeddable Zookeeper server useful for unit testing.
 *
 * @author Randall Hauch
 * @see KafkaCluster
 */
@ThreadSafe
public class ZookeeperServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperServer.class);
    public static int DEFAULT_TICK_TIME = 500;

    /**
     * The basic time unit in milliseconds used by ZooKeeper. It is used to do heartbeats and the minimum session timeout will be
     * twice the tickTime.
     */
    private volatile int tickTime = DEFAULT_TICK_TIME;
    private volatile int port = -1;

    private volatile ServerCnxnFactory factory;
    private volatile File dataDir;
    private volatile File snapshotDir;
    private volatile File logDir;
    private volatile ZooKeeperServer server;

    /**
     * Create a new server instance.
     */
    public ZookeeperServer() {
    }

    /**
     * Start the embedded Zookeeper server.
     *
     * @return this instance to allow chaining methods; never null
     * @throws IOException if there is an error during startup
     * @throws IllegalStateException if the server is already running
     */
    public synchronized ZookeeperServer startup() throws IOException {
        if (factory != null) {
            throw new IllegalStateException("" + this + " is already running");
        }

        if (this.port == -1) {
            this.port = IoUtil.getAvailablePort();
        }
        this.factory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
        if (this.dataDir == null) {
            try {
                File temp = File.createTempFile("kafka", "suffix");
                this.dataDir = temp.getParentFile();
                temp.delete();
            }
            catch (IOException e) {
                throw new RuntimeException("Unable to create temporary directory", e);
            }
        }
        this.snapshotDir = new File(this.dataDir, "snapshot");
        this.logDir = new File(this.dataDir, "log");
        this.snapshotDir.mkdirs();
        this.logDir.mkdirs();

        try {
            server = new ZooKeeperServer(snapshotDir, logDir, tickTime);
            factory.startup(server);
            return this;
        }
        catch (InterruptedException e) {
            factory = null;
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    /**
     * Shutdown the embedded Zookeeper server and delete all data.
     */
    public void shutdown() {
        shutdown(true);
    }

    /**
     * Shutdown the embedded Kafka server.
     *
     * @param deleteData true if the data should be removed, or false otherwise
     */
    public synchronized void shutdown(boolean deleteData) {
        if (factory != null) {
            try {
                factory.shutdown();
                try {
                    // Zookeeper 3.4.6 does not close the ZK DB during shutdown, so we must do this here to avoid file locks and open handles...
                    server.getZKDatabase().close();
                }
                catch (IOException e) {
                    LOGGER.error("Unable to close zookeeper DB", e);
                }
            }
            finally {
                factory = null;
                if (deleteData) {
                    // Delete all data ...
                    try {
                        IoUtil.delete(this.snapshotDir, this.logDir);
                    }
                    catch (IOException e) {
                        LOGGER.error("Unable to delete data upon shutdown", e);
                    }
                }
            }
        }
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
     * Set the port for the server.
     *
     * @param port the desired port, or {@code -1} if a random available port should be found and used
     * @return this instance to allow chaining methods; never null
     */
    public ZookeeperServer setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Set the basic time unit in milliseconds used by ZooKeeper. It is used to do heartbeats and the minimum session timeout will
     * be twice the tickTime.
     *
     * @param tickTime the desired value, or non-positive if the default of {@link #DEFAULT_TICK_TIME} be used
     * @return this instance to allow chaining methods; never null
     */
    public ZookeeperServer setTickTime(int tickTime) {
        this.tickTime = tickTime > 0 ? tickTime : DEFAULT_TICK_TIME;
        return this;
    }

    /**
     * Get the current port.
     *
     * @return the port number, or {@code -1} if the port should be discovered upon {@link #startup()}
     */
    public int getPort() {
        return port;
    }

    /**
     * Get the basic time unit in milliseconds used by ZooKeeper.
     *
     * @return tick time; always positive
     */
    public int getTickTime() {
        return tickTime;
    }

    /**
     * Perform the supplied function on each directory used by this server.
     *
     * @param consumer the consumer function; may not be null
     */
    void onEachDirectory(Consumer<File> consumer) {
        consumer.accept(getSnapshotDirectory());
        consumer.accept(getLogDirectory());
    }

    /**
     * Get the parent directory where the server's snapshots are kept.
     * @return the parent directory for the server's snapshots; never null once the server is running
     */
    File getSnapshotDirectory() {
        return this.snapshotDir;
    }

    /**
     * Get the parent directory where the server's logs are kept.
     * @return the parent directory for the server's logs; never null once the server is running
     */
    File getLogDirectory() {
        return this.logDir;
    }

    /**
     * Get the parent directory where the server's logs and snapshots will be kept.
     * @return the parent directory for the server's logs and snapshots; may be null if a temporary directory will be used
     */
    public File getStateDirectory() {
        return this.logDir;
    }

    /**
     * Set the parent directory where the server's logs and snapshots will be kept.
     * @param dataDir the parent directory for the server's logs and snapshots; may be null if a temporary directory will be used
     * @return this instance to allow method chaining; never null
     * @throws IllegalArgumentException if the supplied file is not a directory or not writable
     */
    public ZookeeperServer setStateDirectory(File dataDir) {
        if (dataDir != null && dataDir.exists() && !dataDir.isDirectory() && !dataDir.canWrite() && !dataDir.canRead()) {
            throw new IllegalArgumentException("The directory must be readable and writable");
        }
        this.dataDir = dataDir;
        return this;
    }

    @Override
    public String toString() {
        return "ZookeeperServer{" + getConnection() + "}";
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.mongodb.connection.MongoDbConnectionContext;
import io.debezium.util.Threads;

/**
 * @author Randall Hauch
 */
public class MongoDbTaskContext extends CdcSourceTaskContext<MongoDbConnectorConfig> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbTaskContext.class);

    private final Filters filters;
    private final String serverName;
    private MongoDbConnectionContext connectionContext;
    private ExecutorService incrementalSnapshotExecutor;
    private boolean closed;

    /**
     * @param config the configuration
     */
    public MongoDbTaskContext(Configuration config) {
        super(config,
                new MongoDbConnectorConfig(config),
                config.getString(MongoDbConnectorConfig.TASK_ID),
                new MongoDbConnectorConfig(config).getCustomMetricTags());

        this.filters = new Filters(config);
        this.serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
    }

    /**
     * Returns the task-owned connection factory shared by snapshot and streaming operations.
     * Each operation remains responsible for closing the clients it creates.
     */
    public synchronized MongoDbConnectionContext getConnectionContext() {
        if (closed) {
            throw new IllegalStateException("MongoDB task context is closed");
        }
        if (connectionContext == null) {
            connectionContext = new MongoDbConnectionContext(getRawConfig());
        }
        return connectionContext;
    }

    /**
     * Returns the task-owned executor for incremental snapshot reads.
     */
    public synchronized ExecutorService getIncrementalSnapshotExecutor() {
        if (closed) {
            throw new IllegalStateException("MongoDB task context is closed");
        }
        if (incrementalSnapshotExecutor == null) {
            incrementalSnapshotExecutor = Threads.newFixedThreadPool(MongoDbConnector.class, getConfig().getConnectorName(),
                    "incremental-snapshot", getConfig().getSnapshotMaxThreads());
        }
        return incrementalSnapshotExecutor;
    }

    @Override
    @SuppressWarnings("try")
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        final boolean interrupted = Thread.interrupted();
        try (var ownedContext = connectionContext) {
            if (incrementalSnapshotExecutor != null) {
                incrementalSnapshotExecutor.shutdownNow();
                try {
                    if (!incrementalSnapshotExecutor.awaitTermination(getConfig().getExecutorShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                        LOGGER.warn("Incremental snapshot workers did not stop within the configured shutdown timeout");
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Filters getFilters() {
        return filters;
    }

    public String getServerName() {
        return serverName;
    }
}

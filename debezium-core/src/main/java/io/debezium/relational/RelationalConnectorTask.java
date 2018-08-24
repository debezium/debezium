/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.OffsetContext.Loader;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Base class for Debezium's CDC connector tasks for relational databases.
 *
 * @author Gunnar Morling
 */
public abstract class RelationalConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalConnectorTask.class);

    private static enum State {
        RUNNING, STOPPED;
    }

    public interface StartingContext {

        RelationalDatabaseConnectorConfig getConnectorConfig();
        Class<? extends SourceConnector> getConnectorClass();
        TopicSelector<TableId> getTopicSelector();
        JdbcConnection getJdbcConnection();
        RelationalDatabaseSchema getDatabaseSchema();
        Loader getOffsetContextLoader();
        ChangeEventSourceFactory getChangeEventSourceFactory(ErrorHandler errorHandler,
                EventDispatcher<TableId> dispatcher, Clock clock);
        Supplier<LoggingContext.PreviousContext> getLoggingContextSupplier();
    }

    private final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile JdbcConnection jdbcConnection;
    private volatile ChangeEventSourceCoordinator coordinator;
    private volatile ErrorHandler errorHandler;
    private volatile RelationalDatabaseSchema schema;
    private volatile Map<String, ?> lastOffset;

    @Override
    public void start(Configuration config) {
        if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
            LOGGER.info("Connector has already been started");
            return;
        }

        StartingContext startingContext = getStartingContext(config);

        RelationalDatabaseConnectorConfig connectorConfig = startingContext.getConnectorConfig();

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(startingContext.getLoggingContextSupplier())
                .build();

        errorHandler = new ErrorHandler(startingContext.getConnectorClass(), connectorConfig.getLogicalName(), queue, this::cleanupResources);
        TopicSelector<TableId> topicSelector = startingContext.getTopicSelector();

        jdbcConnection = startingContext.getJdbcConnection();

        this.schema = startingContext.getDatabaseSchema();

        OffsetContext previousOffset = getPreviousOffset(startingContext.getOffsetContextLoader());
        if (previousOffset != null && schema instanceof HistorizedRelationalDatabaseSchema) {
            ((HistorizedRelationalDatabaseSchema) schema).recover(previousOffset);
        }

        EventDispatcher<TableId> dispatcher = new EventDispatcher<>(connectorConfig, topicSelector, schema, queue,
                connectorConfig.getTableFilters().dataCollectionFilter(), DataChangeEvent::new);

        coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                startingContext.getConnectorClass(),
                connectorConfig.getLogicalName(),
                startingContext.getChangeEventSourceFactory(errorHandler, dispatcher, clock)
        );

        coordinator.start();
    }

    protected abstract StartingContext getStartingContext(Configuration config);

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
            .map(DataChangeEvent::getRecord)
            .collect(Collectors.toList());

        if (!sourceRecords.isEmpty()) {
            this.lastOffset = sourceRecords.get(sourceRecords.size() - 1).sourceOffset();
        }

        return sourceRecords;
    }

    @Override
    public void commit() throws InterruptedException {
        if (lastOffset != null) {
            coordinator.commitOffset(lastOffset);
        }
    }

    @Override
    public void stop() {
        cleanupResources();
    }

    private void cleanupResources() {
        if (!state.compareAndSet(State.RUNNING, State.STOPPED)) {
            LOGGER.info("Connector has already been stopped");
            return;
        }

        try {
            if (coordinator != null) {
                coordinator.stop();
            }
        }
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.error("Interrupted while stopping coordinator", e);
            // XStream code can end in SIGSEGV so fail the task instead of JVM crash
            throw new ConnectException("Interrupted while stopping coordinator, failing the task");
        }

        try {
            if (errorHandler != null) {
                errorHandler.stop();
            }
        }
        catch (InterruptedException e) {
            Thread.interrupted();
            LOGGER.error("Interrupted while stopping", e);
        }

        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        schema.close();
    }
}

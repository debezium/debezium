/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class OracleConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    private static enum State {
        RUNNING, STOPPED;
    }

    private final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);

    private volatile OracleTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile OracleConnection jdbcConnection;
    private volatile ChangeEventSourceCoordinator coordinator;
    private volatile ErrorHandler errorHandler;
    private volatile OracleDatabaseSchema schema;
    private volatile Map<String, ?> lastOffset;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Configuration config) {
        if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
            LOGGER.info("Connector has already been started");
            return;
        }

        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        taskContext = new OracleTaskContext(connectorConfig);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new ErrorHandler(OracleConnector.class, connectorConfig.getLogicalName(), queue, this::cleanupResources);
        TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);

        Configuration jdbcConfig = config.subset("database.", true);

        jdbcConnection = new OracleConnection(jdbcConfig, new OracleConnectionFactory());
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(LOGGER);

        this.schema = new OracleDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, jdbcConnection);

        OffsetContext previousOffset = getPreviousOffset(new OracleOffsetContext.Loader(connectorConfig.getLogicalName()));
        if (previousOffset != null) {
            schema.recover(previousOffset);
        }

        EventDispatcher<TableId> dispatcher = new EventDispatcher<>(connectorConfig, topicSelector, schema, queue,
                connectorConfig.getTableFilters().dataCollectionFilter(), DataChangeEvent::new);

        coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                OracleConnector.class,
                connectorConfig.getLogicalName(),
                new OracleChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema),
                dispatcher
        );

        coordinator.start(taskContext);
    }

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

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return OracleConnectorConfig.ALL_FIELDS;
    }
}

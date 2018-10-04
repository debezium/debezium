/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.Collections;
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
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

/**
 * The main task executing streaming from SQL Server.
 * Responsible for lifecycle management the streaming code.
 *
 * @author Jiri Pechanec
 *
 */
public class SqlServerConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorTask.class);
    private static final String CONTEXT_NAME = "sql-server-connector-task";

    private static enum State {
        RUNNING, STOPPED;
    }

    private final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);

    private volatile SqlServerTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile SqlServerConnection jdbcConnection;
    private volatile ChangeEventSourceCoordinator coordinator;
    private volatile ErrorHandler errorHandler;
    private volatile SqlServerDatabaseSchema schema;
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

        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        taskContext = new SqlServerTaskContext(connectorConfig);

        final Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new ErrorHandler(SqlServerConnector.class, connectorConfig.getLogicalName(), queue, this::cleanupResources);
        final TopicSelector<TableId> topicSelector = SqlServerTopicSelector.defaultSelector(connectorConfig);

        final Configuration jdbcConfig = config.filter(x -> !(x.startsWith(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING) || x.equals(HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name())))
                .subset("database.", true);

        jdbcConnection = new SqlServerConnection(jdbcConfig);
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(LOGGER);

        this.schema = new SqlServerDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, jdbcConnection);

        final OffsetContext previousOffset = getPreviousOffset(new SqlServerOffsetContext.Loader(connectorConfig.getLogicalName()));
        if (previousOffset != null) {
            schema.recover(previousOffset);
        }

        final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new);

        coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                SqlServerConnector.class,
                connectorConfig.getLogicalName(),
                new SqlServerChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema),
                dispatcher
        );

        coordinator.start(taskContext);
    }

    /**
     * Loads the connector's persistent offset (if present) via the given loader.
     */
    @Override
    protected OffsetContext getPreviousOffset(OffsetContext.Loader loader) {
        Map<String, ?> partition = loader.getPartition();

        Map<String, Object> previousOffset = context.offsetStorageReader()
                .offsets(Collections.singleton(partition))
                .get(partition);

        if (previousOffset != null) {
            OffsetContext offsetContext = loader.load(previousOffset);
            LOGGER.info("Found previous offset {}", offsetContext);
            return offsetContext;
        }
        else {
            return null;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
            .map(DataChangeEvent::getRecord)
            .collect(Collectors.toList());

        if (!sourceRecords.isEmpty()) {
            this.lastOffset = sourceRecords.get(sourceRecords.size() - 1).sourceOffset();
        }

        return sourceRecords;
    }

    @Override
    public void commit() throws InterruptedException {
        coordinator.commitOffset(lastOffset);
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
        return SqlServerConnectorConfig.ALL_FIELDS;
    }
}

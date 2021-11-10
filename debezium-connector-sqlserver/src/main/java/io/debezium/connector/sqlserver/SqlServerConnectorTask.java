/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

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
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.spi.Offsets;
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
public class SqlServerConnectorTask extends BaseSourceTask<SqlServerPartition, SqlServerOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerConnectorTask.class);
    private static final String CONTEXT_NAME = "sql-server-connector-task";

    private volatile SqlServerTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile SqlServerConnection dataConnection;
    private volatile SqlServerConnection metadataConnection;
    private volatile ErrorHandler errorHandler;
    private volatile SqlServerDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> start(Configuration config) {
        final Clock clock = Clock.system();
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = SqlServerTopicSelector.defaultSelector(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        final SqlServerValueConverters valueConverters = new SqlServerValueConverters(connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(), connectorConfig.binaryHandlingMode());

        // By default do not load whole result sets into memory
        config = config.edit()
                .withDefault("database.responseBuffering", "adaptive")
                .withDefault("database.fetchSize", 10_000)
                .build();

        final Configuration jdbcConfig = config.filter(
                x -> !(x.startsWith(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING) || x.equals(HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name())))
                .subset("database.", true);
        dataConnection = new SqlServerConnection(jdbcConfig, connectorConfig.getSourceTimestampMode(), valueConverters, () -> getClass().getClassLoader(),
                connectorConfig.getSkippedOperations(), connectorConfig.isMultiPartitionModeEnabled(), connectorConfig.getOptionRecompile());
        metadataConnection = new SqlServerConnection(jdbcConfig, connectorConfig.getSourceTimestampMode(), valueConverters, () -> getClass().getClassLoader(),
                connectorConfig.getSkippedOperations(), connectorConfig.isMultiPartitionModeEnabled());

        this.schema = new SqlServerDatabaseSchema(connectorConfig, metadataConnection.getDefaultValueConverter(), valueConverters, topicSelector, schemaNameAdjuster);
        this.schema.initializeStorage();

        Offsets<SqlServerPartition, SqlServerOffsetContext> offsets = getPreviousOffsets(
                new SqlServerPartition.Provider(connectorConfig),
                new SqlServerOffsetContext.Loader(connectorConfig));
        SqlServerPartition partition = offsets.getTheOnlyPartition();
        SqlServerOffsetContext previousOffset = offsets.getTheOnlyOffset();

        if (previousOffset != null) {
            schema.recover(partition, previousOffset);
        }

        taskContext = new SqlServerTaskContext(connectorConfig, schema);

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new SqlServerErrorHandler(connectorConfig.getLogicalName(), queue);

        final SqlServerEventMetadataProvider metadataProvider = new SqlServerEventMetadataProvider();

        final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                offsets,
                errorHandler,
                SqlServerConnector.class,
                connectorConfig,
                new SqlServerChangeEventSourceFactory(connectorConfig, dataConnection, metadataConnection, errorHandler, dispatcher, clock, schema),
                new DefaultChangeEventSourceMetricsFactory(),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        try {
            if (dataConnection != null) {
                dataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (metadataConnection != null) {
                metadataConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC metadata connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SqlServerConnectorConfig.ALL_FIELDS;
    }
}

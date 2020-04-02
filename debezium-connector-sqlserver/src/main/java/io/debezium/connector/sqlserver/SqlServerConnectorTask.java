/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.List;
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
    public ChangeEventSourceCoordinator start(Configuration config) {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = SqlServerTopicSelector.defaultSelector(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(LOGGER);

        // By default do not load whole result sets into memory
        config = config.edit()
                .withDefault("database.responseBuffering", "adaptive")
                .withDefault("database.fetchSize", 10_000)
                .build();

        final Configuration jdbcConfig = config.filter(
                x -> !(x.startsWith(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING) || x.equals(HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name())))
                .subset("database.", true);
        dataConnection = new SqlServerConnection(jdbcConfig);
        metadataConnection = new SqlServerConnection(jdbcConfig);
        try {
            dataConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
        this.schema = new SqlServerDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, dataConnection);
        this.schema.initializeStorage();

        final OffsetContext previousOffset = getPreviousOffset(new SqlServerOffsetContext.Loader(connectorConfig));
        if (previousOffset != null) {
            schema.recover(previousOffset);
        }

        taskContext = new SqlServerTaskContext(connectorConfig, schema);

        final Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
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
                metadataProvider);

        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                SqlServerConnector.class,
                connectorConfig,
                new SqlServerChangeEventSourceFactory(connectorConfig, dataConnection, metadataConnection, errorHandler, dispatcher, clock, schema),
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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ChangeEventSourceFacade;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.RetryableErrorRecognitionStrategy;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class SqlServerChangeEventSourceFacade implements ChangeEventSourceFacade<SqlServerTaskContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerChangeEventSourceFacade.class);
    private static final String CONTEXT_NAME = "sql-server-connector-task";

    private final Configuration config;
    private final Function<OffsetContext.Loader, OffsetContext> offsetProvider;

    private volatile SqlServerTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile SqlServerConnection dataConnection;
    private volatile SqlServerConnection metadataConnection;
    private volatile ChangeEventSourceCoordinator coordinator;
    private volatile ErrorHandler errorHandler;
    private volatile SqlServerDatabaseSchema schema;

    public SqlServerChangeEventSourceFacade(Configuration config,
                                            Function<OffsetContext.Loader, OffsetContext> offsetProvider) {
        this.config = config.edit()
                .withDefault("database.responseBuffering", "adaptive")
                .withDefault("database.fetchSize", 10_000)
                .build();
        this.offsetProvider = offsetProvider;
    }

    @Override
    public void start() {
        final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = SqlServerTopicSelector.defaultSelector(connectorConfig);
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(LOGGER);

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

        final OffsetContext previousOffset = offsetProvider.apply(new SqlServerOffsetContext.Loader(connectorConfig));
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

        RetryableErrorRecognitionStrategy retryableErrorRecognitionStrategy = error -> false;
        errorHandler = new ErrorHandler(SqlServerConnector.class, connectorConfig.getLogicalName(), this,
                retryableErrorRecognitionStrategy);

        final SqlServerEventMetadataProvider metadataProvider = new SqlServerEventMetadataProvider();

        final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider);

        coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                SqlServerConnector.class,
                connectorConfig,
                new SqlServerChangeEventSourceFactory(connectorConfig, dataConnection, metadataConnection, dispatcher, clock, schema),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        if (coordinator != null) {
            coordinator.commitOffset(offset);
        }
    }

    @Override
    public void stop() {
        try {
            if (coordinator != null) {
                coordinator.stop();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while stopping coordinator", e);
            throw new ConnectException("Interrupted while stopping coordinator, failing the task");
        }

        try {
            if (errorHandler != null) {
                errorHandler.stop();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Interrupted while stopping", e);
        }

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
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public ChangeEventSourceCoordinator getCoordinator() {
        return coordinator;
    }
}

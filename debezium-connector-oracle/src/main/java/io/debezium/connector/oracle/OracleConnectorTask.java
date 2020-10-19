/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

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
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class OracleConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    private volatile OracleTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile OracleConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile OracleDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator start(Configuration config) {
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(LOGGER);

        Configuration jdbcConfig = config.subset("database.", true);
        jdbcConnection = new OracleConnection(jdbcConfig, () -> getClass().getClassLoader());
        this.schema = new OracleDatabaseSchema(connectorConfig, schemaNameAdjuster, topicSelector, jdbcConnection);
        this.schema.initializeStorage();

        String adapterString = config.getString(OracleConnectorConfig.CONNECTOR_ADAPTER);
        OracleConnectorConfig.ConnectorAdapter adapter = OracleConnectorConfig.ConnectorAdapter.parse(adapterString);
        OffsetContext previousOffset = getPreviousOffset(new OracleOffsetContext.Loader(connectorConfig, adapter));

        if (previousOffset != null) {
            schema.recover(previousOffset);
        }

        taskContext = new OracleTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new OracleErrorHandler(connectorConfig.getLogicalName(), queue);

        final OracleEventMetadataProvider metadataProvider = new OracleEventMetadataProvider();

        EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                OracleConnector.class,
                connectorConfig,
                new OracleChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext),
                new DefaultChangeEventSourceMetricsFactory(),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
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

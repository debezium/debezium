/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.QueueProviderService;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.document.DocumentReader;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.GuardrailValidator;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The Kafka Connect source task for ${connectorName}.
 *
 * <p>Wires together all Debezium pipeline components and delegates snapshot/streaming
 * work to {@link ${connectorName}SnapshotChangeEventSource} and
 * {@link ${connectorName}StreamingChangeEventSource} via the coordinator.
 */
public class ${connectorName}ConnectorTask
        extends BaseSourceTask<${connectorName}Partition, ${connectorName}OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(${connectorName}ConnectorTask.class);
    private static final String CONTEXT_NAME = "${connectorName.toLowerCase()}-connector-task";

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile ${connectorName}ConnectorConfig connectorConfig;
    private volatile CdcSourceTaskContext<${connectorName}ConnectorConfig> taskContext;
    private volatile ErrorHandler errorHandler;
    private volatile ${connectorName}Connection beanRegistryJdbcConnection;
    private volatile ${connectorName}Connection jdbcConnection;
    private volatile ${connectorName}DatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    public CdcSourceTaskContext<${connectorName}ConnectorConfig> preStart(Configuration config) {
        connectorConfig = new ${connectorName}ConnectorConfig(config);
        taskContext = new CdcSourceTaskContext<>(config, connectorConfig, Map.of());
        return taskContext;
    }

    @Override
    public ChangeEventSourceCoordinator<${connectorName}Partition, ${connectorName}OffsetContext> start(
            Configuration config) {

        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        // A placeholder table the snapshot/streaming stubs emit against. A real relational
        // connector reads the captured tables from the schema instead of using a fixed id.
        final String collectionName = connectorConfig.getLogicalName();
        final TableId dataCollectionId = new TableId(null, null, collectionName);

        // Restore or initialise offset context.
        final Offsets<${connectorName}Partition, ${connectorName}OffsetContext> previousOffsets =
                getPreviousOffsets(
                        new ${connectorName}Partition.Provider(connectorConfig),
                        new ${connectorName}OffsetLoader(connectorConfig));

        // Register the connector's SPI service providers (queue provider, snapshotter, ...). This must
        // happen before the queue is built because the queue looks up the QueueProviderService.
        registerServiceProviders(connectorConfig.getServiceRegistry());

        // Build the change event queue used to buffer records between threads.
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .pollDispatchInterval(connectorConfig.getPollDispatchInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .queueProvider(connectorConfig.getServiceRegistry().tryGetService(QueueProviderService.class).getQueueProvider())
                .build();

        this.errorHandler = new ${connectorName}ErrorHandler(connectorConfig, queue, errorHandler);

        final TopicNamingStrategy<TableId> topicNamingStrategy =
                connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);

        // The connection factory's mainConnection() is reused for schema reads and the snapshot.
        final MainConnectionProvidingConnectionFactory<${connectorName}Connection> connectionFactory =
                new DefaultMainConnectionProvidingConnectionFactory<>(
                        () -> new ${connectorName}Connection(connectorConfig.getJdbcConfig()));

        jdbcConnection = connectionFactory.mainConnection();

        final JdbcValueConverters valueConverters = new JdbcValueConverters();

        schema =
                new ${connectorName}DatabaseSchema(connectorConfig, topicNamingStrategy, valueConverters, taskContext);

        // A non-historized schema is not persisted, so rebuild it from the database on every start. The
        // snapshot refreshes it too, but it is skipped when a previous offset exists and streaming
        // would otherwise begin without knowing the captured tables.
        try {
            schema.refresh(jdbcConnection);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to read the schema of the captured tables", e);
        }

        final ${connectorName}EventMetadataProvider metadataProvider = new ${connectorName}EventMetadataProvider();

        final SignalProcessor<${connectorName}Partition, ${connectorName}OffsetContext> signalProcessor =
                new SignalProcessor<>(
                        ${connectorName}SourceConnector.class,
                        connectorConfig,
                        Map.of(),
                        getAvailableSignalChannels(),
                        DocumentReader.defaultReader(),
                        previousOffsets);

        final EventDispatcher<${connectorName}Partition, TableId> dispatcher =
                new EventDispatcher<>(
                        connectorConfig,
                        topicNamingStrategy,
                        schema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        new HeartbeatFactory<>().getScheduledHeartbeat(
                                connectorConfig,
                                connectionFactory::newConnection,
                                exception -> {
                                    throw new DebeziumException(
                                            "Could not execute heartbeat action query (Error: " + exception.getMessage() + ")", exception);
                                },
                                queue),
                        schemaNameAdjuster,
                        signalProcessor,
                        null);

        final NotificationService<${connectorName}Partition, ${connectorName}OffsetContext> notificationService =
                new NotificationService<>(
                        getNotificationChannels(),
                        connectorConfig,
                        SchemaFactory.get(),
                        dispatcher::enqueueNotification);

        // Beans the snapshotter service and other framework services look up by name.
        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);

        final SnapshotterService snapshotterService =
                connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        // Refuse to load an excessive number of table schemas into memory (guardrail.collections.max).
        if (connectorConfig.getGuardrailCollectionsMax() <= 0) {
            LOGGER.info("Guardrail validation skipped");
        }
        else {
            validateGuardrailLimits();
        }

        // Fails fast when the stored offset cannot be used: a snapshot that was interrupted but is now
        // disabled, or a log position the source no longer retains.
        validateSchemaHistory(connectorConfig, jdbcConnection::validateLogPosition, previousOffsets, schema,
                snapshotterService.getSnapshotter());

        final ChangeEventSourceCoordinator<${connectorName}Partition, ${connectorName}OffsetContext> coordinator =
                new ChangeEventSourceCoordinator<>(
                        previousOffsets,
                        errorHandler,
                        ${connectorName}SourceConnector.class,
                        connectorConfig,
                        new ${connectorName}ChangeEventSourceFactory(
                                connectorConfig, connectionFactory, schema, dataCollectionId, dispatcher,
                                errorHandler, Clock.system(), snapshotterService),
                        new DefaultChangeEventSourceMetricsFactory<>(),
                        dispatcher,
                        schema,
                        signalProcessor,
                        notificationService,
                        snapshotterService);

        coordinator.start(taskContext, this.queue, metadataProvider);
        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        return pollRecords(queue);
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        }
        catch (Exception e) {
            LOGGER.trace("Error while closing JDBC bean registry connection", e);
        }

        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (Exception e) {
            LOGGER.trace("Error while closing JDBC connection", e);
        }

        if (schema != null) {
            schema.close();
        }

        if (queue != null) {
            queue.close();
        }
    }

    private void validateGuardrailLimits() {
        try {
            final String catalogName = connectorConfig.getJdbcConfig().getString(JdbcConfiguration.DATABASE);
            new GuardrailValidator(connectorConfig, schema).validate(jdbcConnection.getAllTableIds(catalogName));
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to validate guardrail limits", e);
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return ${connectorName}ConnectorConfig.ALL_FIELDS;
    }
}

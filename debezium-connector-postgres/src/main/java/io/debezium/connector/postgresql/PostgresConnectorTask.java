/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresConnection.PostgresValueConverterBuilder;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends BaseSourceTask<PostgresPartition, PostgresOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnectorTask.class);
    private static final String CONTEXT_NAME = "postgres-connector-task";

    private volatile PostgresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile PostgresConnection beanRegistryJdbcConnection;
    private volatile ReplicationConnection replicationConnection = null;

    private volatile ErrorHandler errorHandler;
    private volatile PostgresSchema schema;

    @Override
    public ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> start(Configuration config) {
        final PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY);
        final Snapshotter snapshotter = connectorConfig.getSnapshotter();
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        if (snapshotter == null) {
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode, double check your settings");
        }

        final Charset databaseCharset;
        try (PostgresConnection tempConnection = new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL)) {
            databaseCharset = tempConnection.getDatabaseCharset();
        }

        final PostgresValueConverterBuilder valueConverterBuilder = (typeRegistry) -> PostgresValueConverter.of(
                connectorConfig,
                databaseCharset,
                typeRegistry);

        MainConnectionProvidingConnectionFactory<PostgresConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new PostgresConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder, PostgresConnection.CONNECTION_GENERAL));
        // Global JDBC connection used both for snapshotting and streaming.
        // Must be able to resolve datatypes.
        jdbcConnection = connectionFactory.mainConnection();
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
        final PostgresDefaultValueConverter defaultValueConverter = jdbcConnection.getDefaultValueConverter();
        final PostgresValueConverter valueConverter = valueConverterBuilder.build(typeRegistry);

        schema = new PostgresSchema(connectorConfig, defaultValueConverter, topicNamingStrategy, valueConverter);
        this.taskContext = new PostgresTaskContext(connectorConfig, schema, topicNamingStrategy);
        final Offsets<PostgresPartition, PostgresOffsetContext> previousOffsets = getPreviousOffsets(
                new PostgresPartition.Provider(connectorConfig, config), new PostgresOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();
        final PostgresOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        // Manual Bean Registration
        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverter);

        // Service providers
        registerServiceProviders(connectorConfig.getServiceRegistry());

        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            SlotState slotInfo = null;
            try {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(jdbcConnection.serverInfo().toString());
                }
                slotInfo = jdbcConnection.getReplicationSlotState(connectorConfig.slotName(), connectorConfig.plugin().getPostgresPluginName());
            }
            catch (SQLException e) {
                LOGGER.warn("unable to load info of replication slot, Debezium will try to create the slot", e);
                throw e;
            }

            if (previousOffset == null) {
                LOGGER.info("No previous offset found");
                // if we have no initial offset, indicate that to Snapshotter by passing null
                snapshotter.init(connectorConfig, null, slotInfo);
            }
            else {
                LOGGER.info("Found previous offset {}", previousOffset);
                snapshotter.init(connectorConfig, previousOffset.asOffsetState(), slotInfo);
            }

            SlotCreationResult slotCreatedInfo = null;
            if (snapshotter.shouldStream() || (YugabyteDBServer.isEnabled() && (slotInfo == null))) {
                replicationConnection = createReplicationConnection(this.taskContext,
                        connectorConfig.maxRetries(), connectorConfig.retryDelay());

                // we need to create the slot before we start streaming if it doesn't exist
                // otherwise we can't stream back changes happening while the snapshot is taking place
                if (slotInfo == null) {
                    try {
                        slotCreatedInfo = replicationConnection.createReplicationSlot().orElse(null);
                    }
                    catch (SQLException ex) {
                        String message = "Creation of replication slot failed";
                        if (ex.getMessage().contains("already exists")) {
                            message += "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                        }
                        throw new DebeziumException(message, ex);
                    }
                }
                else {
                    slotCreatedInfo = null;
                }
            }

            try {
                jdbcConnection.commit();
            }
            catch (SQLException e) {
                throw new DebeziumException(e);
            }

            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            errorHandler = new PostgresErrorHandler(connectorConfig, queue, errorHandler);

            final PostgresEventMetadataProvider metadataProvider = new PostgresEventMetadataProvider();

            SignalProcessor<PostgresPartition, PostgresOffsetContext> signalProcessor = new SignalProcessor<>(
                    PostgresConnector.class, connectorConfig, Map.of(),
                    getAvailableSignalChannels(),
                    DocumentReader.defaultReader(),
                    previousOffsets);

            final PostgresEventDispatcher<TableId> dispatcher = new PostgresEventDispatcher<>(
                    connectorConfig,
                    topicNamingStrategy,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    PostgresChangeRecordEmitter::updateSchema,
                    metadataProvider,
                    connectorConfig.createHeartbeat(
                            topicNamingStrategy,
                            schemaNameAdjuster,
                            () -> new PostgresConnection(connectorConfig.getJdbcConfig(), PostgresConnection.CONNECTION_GENERAL),
                            exception -> {
                                String sqlErrorId = exception.getSQLState();
                                switch (sqlErrorId) {
                                    case "57P01":
                                        // Postgres error admin_shutdown, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                        throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    case "57P03":
                                        // Postgres error cannot_connect_now, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                        throw new RetriableException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                                    default:
                                        break;
                                }
                            }),
                    schemaNameAdjuster,
                    signalProcessor);

            NotificationService<PostgresPartition, PostgresOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                    connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

            ChangeEventSourceCoordinator<PostgresPartition, PostgresOffsetContext> coordinator = new PostgresChangeEventSourceCoordinator(
                    previousOffsets,
                    errorHandler,
                    PostgresConnector.class,
                    connectorConfig,
                    new PostgresChangeEventSourceFactory(
                            connectorConfig,
                            snapshotter,
                            connectionFactory,
                            errorHandler,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            replicationConnection,
                            slotCreatedInfo,
                            slotInfo),
                    new DefaultChangeEventSourceMetricsFactory<>(),
                    dispatcher,
                    schema,
                    snapshotter,
                    slotInfo,
                    signalProcessor,
                    notificationService);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        } catch (Exception exception) {
            // YB Note: Catch all the exceptions and retry.
            throw new RetriableException(exception);
        }
        finally {
            previousContext.restore();
        }
    }

    public ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext, int maxRetries, Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(jdbcConnection);
            }
            catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Too many errors connecting to server. All {} retries failed.", maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn("Error connecting to server; will attempt retry {} of {} after {} " +
                        "seconds. Exception message: {}", retryCount, maxRetries, retryDelay.getSeconds(), ex.getMessage());
                try {
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return replicationConnection;
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
        // The replication connection is regularly closed at the end of streaming phase
        // in case of error it can happen that the connector is terminated before the stremaing
        // phase is started. It can lead to a leaked connection.
        // This is guard to make sure the connection is closed.
        try {
            if (replicationConnection != null) {
                replicationConnection.close();
            }
        }
        catch (Exception e) {
            LOGGER.trace("Error while closing replication connection", e);
        }

        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        }
        catch (Exception e) {
            LOGGER.trace("Error while closing JDBC bean registry connection", e);
        }

        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }

    public PostgresTaskContext getTaskContext() {
        return taskContext;
    }
}

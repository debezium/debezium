/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends BaseSourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnectorTask.class);
    private static final String CONTEXT_NAME = "postgres-connector-task";

    private volatile PostgresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile PostgresConnection heartbeatConnection;
    private volatile PostgresSchema schema;

    @Override
    public ChangeEventSourceCoordinator start(Configuration config) {
        final PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig);
        final Snapshotter snapshotter = connectorConfig.getSnapshotter();
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        if (snapshotter == null) {
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode, double check your settings");
        }

        // Global JDBC connection used both for snapshotting and streaming.
        // Must be able to resolve datatypes.
        jdbcConnection = new PostgresConnection(connectorConfig.jdbcConfig(), true);
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
        heartbeatConnection = new PostgresConnection(connectorConfig.jdbcConfig());
        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
        final Charset databaseCharset = jdbcConnection.getDatabaseCharset();

        schema = new PostgresSchema(connectorConfig, typeRegistry, databaseCharset, topicSelector);
        this.taskContext = new PostgresTaskContext(connectorConfig, schema, topicSelector);
        final PostgresOffsetContext previousOffset = (PostgresOffsetContext) getPreviousOffset(new PostgresOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();

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
                LOGGER.warn("unable to load info of replication slot, Debezium will try to create the slot");
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

            ReplicationConnection replicationConnection = null;
            SlotCreationResult slotCreatedInfo = null;
            if (snapshotter.shouldStream()) {
                final boolean shouldExport = snapshotter.exportSnapshot();
                final boolean doSnapshot = snapshotter.shouldSnapshot();
                replicationConnection = createReplicationConnection(this.taskContext, shouldExport,
                        doSnapshot, connectorConfig.maxRetries(), connectorConfig.retryDelay());

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

            ErrorHandler errorHandler = new PostgresErrorHandler(connectorConfig.getLogicalName(), queue);

            final PostgresEventMetadataProvider metadataProvider = new PostgresEventMetadataProvider();

            Configuration configuration = connectorConfig.getConfig();
            Heartbeat heartbeat = Heartbeat.create(
                    configuration.getDuration(Heartbeat.HEARTBEAT_INTERVAL, ChronoUnit.MILLIS),
                    configuration.getString(DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY),
                    topicSelector.getHeartbeatTopic(),
                    connectorConfig.getLogicalName(), heartbeatConnection, exception -> {
                        String sqlErrorId = exception.getSQLState();
                        switch (sqlErrorId) {
                            case "57P01":
                                // Postgres error admin_shutdown, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                throw new DebeziumException("Could not execute heartbeat action (Error: " + sqlErrorId + ")", exception);
                            case "57P03":
                                // Postgres error cannot_connect_now, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                throw new RetriableException("Could not execute heartbeat action (Error: " + sqlErrorId + ")", exception);
                            default:
                                break;
                        }
                    });

            final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicSelector,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    PostgresChangeRecordEmitter::updateSchema,
                    metadataProvider,
                    heartbeat,
                    schemaNameAdjuster);

            ChangeEventSourceCoordinator coordinator = new PostgresChangeEventSourceCoordinator(
                    previousOffset,
                    errorHandler,
                    PostgresConnector.class,
                    connectorConfig,
                    new PostgresChangeEventSourceFactory(
                            connectorConfig,
                            snapshotter,
                            jdbcConnection,
                            errorHandler,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            replicationConnection,
                            slotCreatedInfo,
                            slotInfo),
                    new DefaultChangeEventSourceMetricsFactory(),
                    dispatcher,
                    schema,
                    snapshotter,
                    slotInfo);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    public ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext, boolean shouldExport,
                                                             boolean doSnapshot, int maxRetries, Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(shouldExport, doSnapshot);
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
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (heartbeatConnection != null) {
            heartbeatConnection.close();
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

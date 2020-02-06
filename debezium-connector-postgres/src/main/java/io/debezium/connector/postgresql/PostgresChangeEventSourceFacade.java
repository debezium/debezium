/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ChangeEventSourceFacade;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.RetryableErrorRecognitionStrategy;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.ExceptionUtils;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;

public class PostgresChangeEventSourceFacade implements ChangeEventSourceFacade<PostgresTaskContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresChangeEventSourceFacade.class);
    private static final String CONTEXT_NAME = "postgres-connector-task";

    private final Configuration config;
    private final Function<OffsetContext.Loader, OffsetContext> offsetProvider;

    private volatile PostgresTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile PostgresConnection jdbcConnection;
    private volatile ChangeEventSourceCoordinator coordinator;
    private volatile ErrorHandler errorHandler;
    private volatile PostgresSchema schema;

    public PostgresChangeEventSourceFacade(Configuration config, Function<OffsetContext.Loader, OffsetContext> offsetProvider) {
        this.config = config;
        this.offsetProvider = offsetProvider;
    }

    @Override
    public void start() {
        final PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig);
        final Snapshotter snapshotter = connectorConfig.getSnapshotter();

        if (snapshotter == null) {
            throw new ConnectException(
                    "Unable to load snapshotter, if using custom snapshot mode, double check your settings");
        }

        jdbcConnection = new PostgresConnection(connectorConfig.jdbcConfig());
        final TypeRegistry typeRegistry = jdbcConnection.getTypeRegistry();
        final Charset databaseCharset = jdbcConnection.getDatabaseCharset();

        schema = new PostgresSchema(connectorConfig, typeRegistry, databaseCharset, topicSelector);
        this.taskContext = new PostgresTaskContext(connectorConfig, schema, topicSelector);
        final PostgresOffsetContext previousOffset = (PostgresOffsetContext) offsetProvider.apply(
                new PostgresOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();

        final SourceInfo sourceInfo = new SourceInfo(connectorConfig);
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            SlotState slotInfo = null;
            try (PostgresConnection connection = taskContext.createConnection()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(connection.serverInfo().toString());
                }
                slotInfo = connection.getReplicationSlotState(connectorConfig.slotName(),
                        connectorConfig.plugin().getPostgresPluginName());
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
                LOGGER.info("Found previous offset {}", sourceInfo);
                snapshotter.init(connectorConfig, previousOffset.asOffsetState(), slotInfo);
            }

            ReplicationConnection replicationConnection = null;
            SlotCreationResult slotCreatedInfo = null;
            if (snapshotter.shouldStream()) {
                boolean shouldExport = snapshotter.exportSnapshot();
                replicationConnection = createReplicationConnection(this.taskContext, shouldExport,
                        connectorConfig.maxRetries(), connectorConfig.retryDelay());

                // we need to create the slot before we start streaming if it doesn't exist
                // otherwise we can't stream back changes happening while the snapshot is taking place
                if (slotInfo == null) {
                    try {
                        slotCreatedInfo = replicationConnection.createReplicationSlot().orElse(null);
                    }
                    catch (SQLException ex) {
                        throw new ConnectException(ex);
                    }
                }
                else {
                    slotCreatedInfo = null;
                }
            }

            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            RetryableErrorRecognitionStrategy retryableErrorRecognitionStrategy = error -> ExceptionUtils.testInExceptionHierarchy(error,
                    possibleCause -> (possibleCause instanceof PSQLException)
                            && possibleCause.getMessage().startsWith("Database connection failed"));
            errorHandler = new ErrorHandler(PostgresConnector.class, connectorConfig.getLogicalName(), this,
                    retryableErrorRecognitionStrategy);

            final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicSelector,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    PostgresChangeRecordEmitter::updateSchema);

            coordinator = new ChangeEventSourceCoordinator(
                    previousOffset,
                    errorHandler,
                    PostgresConnector.class,
                    connectorConfig.getLogicalName(),
                    new PostgresChangeEventSourceFactory(
                            connectorConfig,
                            snapshotter,
                            jdbcConnection,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            replicationConnection,
                            slotCreatedInfo),
                    dispatcher,
                    schema);

            coordinator.start(taskContext, this.queue, new PostgresEventMetadataProvider());
        }
        finally {
            previousContext.restore();
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        if (coordinator != null) {
            coordinator.commitOffset(offset);
        }
    }

    public static ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext,
                                                                    boolean shouldExport,
                                                                    int maxRetries, Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(shouldExport);
            }
            catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Too many errors connecting to server. All {} retries failed.", maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn("Error connecting to server; will attempt retry {} of {} after {} " +
                        "seconds. Exception message: {}", retryCount, maxRetries, retryDelay.getSeconds(),
                        ex.getMessage());
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

        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    PostgresTaskContext getTaskContext() {
        return taskContext;
    }
}

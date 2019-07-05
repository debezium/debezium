/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.LoggingContext;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends BaseSourceTask {

    private static final String CONTEXT_NAME = "postgres-connector-task";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean running = new AtomicBoolean(false);

    private PostgresTaskContext taskContext;
    private RecordsProducer producer;

    /**
     * In case of wal2json, all records of one TX will be sent with the same LSN. This is the last LSN that was
     * completely processed, i.e. we've seen all events originating from that TX.
     */
    private volatile Long lastCompletelyProcessedLsn;

    /**
     * A queue with change events filled by the snapshot and streaming producers, consumed
     * by Kafka Connect via this task.
     */
    private ChangeEventQueue<ChangeEvent> changeEventQueue;

    @Override
    public void start(Configuration config) {
        if (running.get()) {
            // already running
            return;
        }

        PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);

        TypeRegistry typeRegistry;
        Charset databaseCharset;

        try (final PostgresConnection connection = new PostgresConnection(connectorConfig.jdbcConfig())) {
            typeRegistry = connection.getTypeRegistry();
            databaseCharset = connection.getDatabaseCharset();
        }

        Snapshotter snapshotter = connectorConfig.getSnapshotter();
        if (snapshotter == null) {
            logger.error("Unable to load snapshotter, if using custom snapshot mode, double check your settings");
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode, double check your settings");
        }

        // create the task context and schema...
        TopicSelector<TableId> topicSelector = PostgresTopicSelector.create(connectorConfig);
        PostgresSchema schema = new PostgresSchema(connectorConfig, typeRegistry, databaseCharset, topicSelector);
        this.taskContext = new PostgresTaskContext(connectorConfig, schema, topicSelector);

        SourceInfo sourceInfo = new SourceInfo(connectorConfig);
        Map<String, Object> existingOffset = context.offsetStorageReader().offset(sourceInfo.partition());
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            //Print out the server information
            SlotState slotInfo = null;
            try (PostgresConnection connection = taskContext.createConnection()) {
                if (logger.isInfoEnabled()) {
                    logger.info(connection.serverInfo().toString());
                }
                slotInfo = connection.getReplicationSlotState(connectorConfig.slotName(), connectorConfig.plugin().getPostgresPluginName());
            }
            catch (SQLException e) {
                logger.warn("unable to load info of replication slot, debezium will try to create the slot");
            }

            if (existingOffset == null) {
                logger.info("No previous offset found");
                // if we have no initial offset, indicate that to Snapshotter by passing null
                snapshotter.init(connectorConfig, null, slotInfo);
            }
            else {
                logger.info("Found previous offset {}", sourceInfo);
                sourceInfo.load(existingOffset);
                snapshotter.init(connectorConfig, sourceInfo.asOffsetState(), slotInfo);
            }

            createRecordProducer(taskContext, sourceInfo, snapshotter);

            changeEventQueue = new ChangeEventQueue.Builder<ChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

            producer.start(changeEventQueue::enqueue, changeEventQueue::producerFailure);
            running.compareAndSet(false, true);
        }
        finally {
            previousContext.restore();
        }
    }

    private void createRecordProducer(PostgresTaskContext taskContext, SourceInfo sourceInfo, Snapshotter snapshotter) {
        if (snapshotter.shouldSnapshot()) {
            if (snapshotter.shouldStream()) {
                logger.info("Taking a new snapshot of the DB and streaming logical changes once the snapshot is finished...");
                producer = new RecordsSnapshotProducer(taskContext, sourceInfo, snapshotter);
            }
            else {
                logger.info("Taking only a snapshot of the DB without streaming any changes afterwards...");
                producer = new RecordsSnapshotProducer(taskContext, sourceInfo, snapshotter);
            }
        }
        else if (snapshotter.shouldStream()) {
            logger.info("Not attempting to take a snapshot, immediately starting to stream logical changes...");
            producer = new RecordsStreamProducer(taskContext, sourceInfo);
        }
        else {
            throw new ConnectException("Snapshotter neither is snapshotting or streaming, invalid!");
        }
    }

    @Override
    public void commit() throws InterruptedException {
        if (running.get()) {
            if (lastCompletelyProcessedLsn != null) {
                producer.commit(lastCompletelyProcessedLsn);
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<ChangeEvent> events = changeEventQueue.poll();

        if (events.size() > 0) {
            lastCompletelyProcessedLsn = events.get(events.size() - 1).getLastCompletelyProcessedLsn();
        }
        return events.stream().map(ChangeEvent::getRecord).collect(Collectors.toList());
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            producer.stop();
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
}

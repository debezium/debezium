/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.openlineage.dataset.DatasetMetadata.TABLE_DATASET_TYPE;
import static io.debezium.openlineage.dataset.DatasetMetadata.DataStore.DATABASE;
import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetKind.OUTPUT;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hibernate.JDBCException;
import org.hibernate.StatelessSession;
import org.hibernate.dialect.DatabaseVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.metadata.CollectionId;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.spi.ChangeEventSink;
import io.debezium.util.Stopwatch;

/**
 * A {@link ChangeEventSink} for a JDBC relational database.
 *
 * @author Chris Cranford
 */
public class JdbcChangeEventSink implements ChangeEventSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeEventSink.class);
    public static final String FOUND_SCHEMA_CHANGE_RECORD_MSG = "Schema change records are not supported by JDBC connector. Adjust `topics` or `topics.regex` to exclude schema change topic.";

    private final JdbcSinkConnectorConfig config;
    private final DatabaseDialect dialect;
    private final StatelessSession session;

    private final RecordWriter recordWriter;
    private final ConnectorContext connectorContext;

    public JdbcChangeEventSink(JdbcSinkConnectorConfig config, StatelessSession session, DatabaseDialect dialect, RecordWriter recordWriter,
                               ConnectorContext connectorContext) {
        this.config = config;
        this.dialect = dialect;
        this.session = session;
        this.recordWriter = recordWriter;
        this.connectorContext = connectorContext;

        final DatabaseVersion version = this.dialect.getVersion();
        LOGGER.info("Database version {}.{}.{}", version.getMajor(), version.getMinor(), version.getMicro());
    }

    public void execute(Collection<SinkRecord> records) {
        final Map<CollectionId, Buffer> upsertBufferByTable = new LinkedHashMap<>();
        final Map<CollectionId, Buffer> deleteBufferByTable = new LinkedHashMap<>();

        for (SinkRecord kafkaSinkRecord : records) {
            JdbcSinkRecord record = new JdbcKafkaSinkRecord(kafkaSinkRecord, config);
            LOGGER.trace("Processing {}", record);

            if (record.isSchemaChange()) {
                LOGGER.error("Ignored schema change event for topic '{}'. " + FOUND_SCHEMA_CHANGE_RECORD_MSG, record.topicName());
                continue;
            }

            CollectionId collectionId = getCollectionIdFromRecord(record);
            if (null == collectionId) {
                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'. No resolvable table name", record.topicName(), record.partition(),
                        record.offset());
                continue;
            }

            if (record.isTruncate()) {
                if (!config.isTruncateEnabled()) {
                    LOGGER.debug("Truncates are not enabled, skipping truncate for topic '{}'", record.topicName());
                    continue;
                }

                // Here we want to flush the buffers to let truncate having effect on the buffered events.
                flushBuffers(upsertBufferByTable);
                flushBuffers(deleteBufferByTable);

                try {
                    final TableDescriptor table = recordWriter.checkAndApplyTableChangesIfNeeded(collectionId, record);
                    recordWriter.writeTruncate(table.getId());
                    continue;
                }
                catch (SQLException | JDBCException e) {
                    throw new ConnectException("Failed to process a sink record", e);
                }
            }

            if (record.isDelete() || record.isTombstone()) {
                if (!config.isDeleteEnabled()) {
                    LOGGER.debug("Deletes are not enabled, skipping delete for topic '{}'", record.topicName());
                    continue;
                }

                final Buffer upsertBufferToFlush = upsertBufferByTable.get(collectionId);
                if (upsertBufferToFlush != null && !upsertBufferToFlush.isEmpty()) {
                    // When a delete event arrives, update buffer must be flushed to avoid losing the delete
                    // for the same record after its update.
                    if (config.isUseReductionBuffer()) {
                        upsertBufferToFlush.remove(record);
                    }
                    else {
                        flushBufferWithRetries(collectionId, upsertBufferToFlush);
                    }
                }

                flushBufferRecordsWithRetries(collectionId, getRecordsToFlush(deleteBufferByTable, collectionId, record));
            }
            else {
                final Buffer deleteBufferToFlush = deleteBufferByTable.get(collectionId);
                if (deleteBufferToFlush != null && !deleteBufferToFlush.isEmpty()) {
                    // When an insert arrives, delete buffer must be flushed to avoid losing an insert for the same record after its deletion.
                    // this because at the end we will always flush inserts before deletes.
                    if (config.isUseReductionBuffer()) {
                        deleteBufferToFlush.remove(record);
                    }
                    else {
                        flushBufferWithRetries(collectionId, deleteBufferToFlush);
                    }
                }

                flushBufferRecordsWithRetries(collectionId, getRecordsToFlush(upsertBufferByTable, collectionId, record));
            }
        }

        flushBuffers(upsertBufferByTable);
        flushBuffers(deleteBufferByTable);
    }

    private BufferFlushRecords getRecordsToFlush(Map<CollectionId, Buffer> bufferMap, CollectionId collectionId, JdbcSinkRecord record) {
        Stopwatch stopwatch = Stopwatch.reusable();
        stopwatch.start();

        Buffer buffer = getOrCreateBuffer(bufferMap, collectionId, record);

        if (isSchemaChanged(record, buffer.getTableDescriptor())) {
            flushBufferWithRetries(collectionId, buffer);

            // Explicitly remove as we need to recreate the buffer
            bufferMap.remove(collectionId);

            buffer = getOrCreateBuffer(bufferMap, collectionId, record);
        }

        List<JdbcSinkRecord> toFlush = buffer.add(record);
        stopwatch.stop();

        LOGGER.trace("[PERF] Resolve and add record execution time for collection '{}': {}", collectionId.name(), stopwatch.durations());

        return new BufferFlushRecords(buffer, toFlush);
    }

    private Buffer getOrCreateBuffer(Map<CollectionId, Buffer> bufferMap, CollectionId collectionId, JdbcSinkRecord record) {
        return bufferMap.computeIfAbsent(collectionId, (id) -> {
            final TableDescriptor tableDescriptor;
            try {
                tableDescriptor = recordWriter.checkAndApplyTableChangesIfNeeded(collectionId, record);
            }
            catch (SQLException | JDBCException e) {
                throw new ConnectException("Error while checking and applying table changes for collection '" + collectionId + "'", e);
            }
            return createBuffer(config, tableDescriptor, record);
        });
    }

    // Describes a specific buffer and a potential subset of records in the buffer to be flushed
    private record BufferFlushRecords(Buffer buffer, List<JdbcSinkRecord> records) {
    }

    private Buffer createBuffer(JdbcSinkConnectorConfig config, TableDescriptor tableDescriptor, JdbcSinkRecord record) {
        if (config.isUseReductionBuffer() && !record.keyFieldNames().isEmpty()) {
            return new ReducedRecordBuffer(config, tableDescriptor);
        }
        else {
            return new RecordBuffer(config, tableDescriptor);
        }
    }

    private boolean isSchemaChanged(JdbcSinkRecord record, TableDescriptor tableDescriptor) {
        Set<String> missingFields = dialect.resolveMissingFields(record, tableDescriptor);
        LOGGER.debug("Schema change detected for '{}', missing fields: {}", tableDescriptor.getId().toFullIdentiferString(), missingFields);
        return !missingFields.isEmpty();
    }

    private void flushBuffers(Map<CollectionId, Buffer> bufferByTable) {
        bufferByTable.forEach(this::flushBufferWithRetries);
    }

    private void flushBufferRecordsWithRetries(CollectionId collectionId, BufferFlushRecords bufferFlushRecords) {
        flushBufferWithRetries(collectionId, bufferFlushRecords.records(), bufferFlushRecords.buffer.getTableDescriptor());
    }

    private void flushBufferWithRetries(CollectionId collectionId, Buffer buffer) {
        flushBufferWithRetries(collectionId, buffer.flush(), buffer.getTableDescriptor());
    }

    private void flushBufferWithRetries(CollectionId collectionId, List<JdbcSinkRecord> toFlush, TableDescriptor tableDescriptor) {
        LOGGER.debug("Flushing records in JDBC Writer for table: {}", collectionId.name());
        recordWriter.executeWithRetries("flush records for table '" + collectionId.name() + "'", () -> {
            flushBuffer(collectionId, toFlush, tableDescriptor);
            return null;
        });
    }

    private void flushBuffer(CollectionId collectionId, List<JdbcSinkRecord> toFlush, TableDescriptor table) throws SQLException {
        Stopwatch flushBufferStopwatch = Stopwatch.reusable();
        Stopwatch tableChangesStopwatch = Stopwatch.reusable();
        if (!toFlush.isEmpty()) {
            LOGGER.debug("Flushing {} records in JDBC Writer for table: {}", toFlush.size(), collectionId.name());
            tableChangesStopwatch.start();
            tableChangesStopwatch.stop();
            flushBufferStopwatch.start();
            recordWriter.write(table, toFlush);
            flushBufferStopwatch.stop();

            DebeziumOpenLineageEmitter.emit(connectorContext, DebeziumTaskState.RUNNING, List.of(extractDatasetMetadata(table)));

            LOGGER.trace("[PERF] Flush buffer execution time {}", flushBufferStopwatch.durations());
            LOGGER.trace("[PERF] Table changes execution time {}", tableChangesStopwatch.durations());
        }
    }

    private DatasetMetadata extractDatasetMetadata(TableDescriptor tableDescriptor) {

        List<DatasetMetadata.FieldDefinition> fieldDefinitions = tableDescriptor.getColumns().stream()
                .map(c -> new DatasetMetadata.FieldDefinition(c.getColumnName(), c.getTypeName(), ""))
                .toList();
        return new DatasetMetadata(getIdentifier(tableDescriptor), OUTPUT, TABLE_DATASET_TYPE, DATABASE, fieldDefinitions);
    }

    private String getIdentifier(TableDescriptor tableDescriptor) {

        return tableDescriptor.getId().toFullIdentiferString();
    }

    @Override
    public void close() {
        if (session != null && session.isOpen()) {
            LOGGER.info("Closing session.");
            session.close();
        }
        else {
            LOGGER.info("Session already closed.");
        }
    }

    public CollectionId getCollectionId(String collectionName) {
        return dialect.getCollectionId(collectionName);
    }

    public CollectionId getCollectionIdFromRecord(DebeziumSinkRecord record) {
        String tableName = this.config.getCollectionNamingStrategy().resolveCollectionName(record, config.getCollectionNameFormat());
        if (tableName == null) {
            return null;
        }
        return getCollectionId(tableName);
    }
}

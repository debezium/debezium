/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE;
import static io.debezium.connector.jdbc.JdbcSinkRecord.FieldDescriptor;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.spi.ChangeEventSink;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;

/**
 * A {@link ChangeEventSink} for a JDBC relational database.
 *
 * @author Chris Cranford
 */
public class JdbcChangeEventSink implements ChangeEventSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeEventSink.class);

    public static final String DETECT_SCHEMA_CHANGE_RECORD_MSG = "Schema change records are not supported by JDBC connector. Adjust `topics` or `topics.regex` to exclude schema change topic.";

    private final JdbcSinkConnectorConfig config;
    private final DatabaseDialect dialect;
    private final StatelessSession session;

    private final RecordWriter recordWriter;
    private final int flushMaxRetries;
    private final Duration flushRetryDelay;

    public JdbcChangeEventSink(JdbcSinkConnectorConfig config, StatelessSession session, DatabaseDialect dialect, RecordWriter recordWriter) {
        this.config = config;
        this.dialect = dialect;
        this.session = session;
        this.recordWriter = recordWriter;
        this.flushMaxRetries = config.getFlushMaxRetries();
        this.flushRetryDelay = Duration.of(config.getFlushRetryDelayMs(), ChronoUnit.MILLIS);

        final DatabaseVersion version = this.dialect.getVersion();
        LOGGER.info("Database version {}.{}.{}", version.getMajor(), version.getMinor(), version.getMicro());
    }

    public void execute(Collection<SinkRecord> records) {
        final Map<CollectionId, Buffer> upsertBufferByTable = new LinkedHashMap<>();
        final Map<CollectionId, Buffer> deleteBufferByTable = new LinkedHashMap<>();

        for (SinkRecord kafkaSinkRecord : records) {

            JdbcSinkRecord record = new JdbcKafkaSinkRecord(kafkaSinkRecord, config.getPrimaryKeyMode(), config.getPrimaryKeyFields(), config.getFieldFilter(), dialect);
            LOGGER.trace("Processing {}", record);

            validate(record);

            Optional<CollectionId> optionalCollectionId = getCollectionIdFromRecord(record);
            if (optionalCollectionId.isEmpty()) {

                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'. No resolvable table name", record.topicName(), record.partition(),
                        record.offset());
                continue;
            }

            final CollectionId collectionId = optionalCollectionId.get();

            if (record.isTruncate()) {
                if (!config.isTruncateEnabled()) {
                    LOGGER.debug("Truncates are not enabled, skipping truncate for topic '{}'", record.topicName());
                    continue;
                }

                // Here we want to flush the buffer to let truncate having effect on the buffered events.
                flushBuffers(upsertBufferByTable);
                flushBuffers(deleteBufferByTable);

                try {
                    final TableDescriptor table = checkAndApplyTableChangesIfNeeded(collectionId, record);
                    writeTruncate(dialect.getTruncateStatement(table));
                    continue;
                }
                catch (SQLException e) {
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

    private void validate(JdbcSinkRecord record) {
        if (record.isSchemaChange()) {
            LOGGER.error(DETECT_SCHEMA_CHANGE_RECORD_MSG);
            throw new DataException(DETECT_SCHEMA_CHANGE_RECORD_MSG);
        }
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
                tableDescriptor = checkAndApplyTableChangesIfNeeded(collectionId, record);
            }
            catch (SQLException e) {
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
        int retries = 0;
        Exception lastException = null;

        LOGGER.debug("Flushing records in JDBC Writer for table: {}", collectionId.name());
        while (retries <= flushMaxRetries) {
            try {
                if (retries > 0) {
                    LOGGER.warn("Retry to flush records for table '{}'. Retry {}/{} with delay {} ms",
                            collectionId.name(), retries, flushMaxRetries, flushRetryDelay.toMillis());
                    try {
                        Metronome.parker(flushRetryDelay, Clock.SYSTEM).pause();
                    }
                    catch (InterruptedException e) {
                        throw new ConnectException("Interrupted while waiting to retry flush records", e);
                    }
                }
                flushBuffer(collectionId, toFlush, tableDescriptor);
                return;
            }
            catch (Exception e) {
                lastException = e;
                if (isRetriable(e)) {
                    retries++;
                }
                else {
                    throw new ConnectException("Failed to process a sink record", e);
                }
            }
        }
        throw new ConnectException("Exceeded max retries " + flushMaxRetries + " times, failed to process sink records", lastException);
    }

    private void flushBuffer(CollectionId collectionId, List<JdbcSinkRecord> toFlush, TableDescriptor table) throws SQLException {
        Stopwatch flushBufferStopwatch = Stopwatch.reusable();
        Stopwatch tableChangesStopwatch = Stopwatch.reusable();
        if (!toFlush.isEmpty()) {
            LOGGER.debug("Flushing records in JDBC Writer for table: {}", collectionId.name());
            tableChangesStopwatch.start();
            tableChangesStopwatch.stop();
            String sqlStatement = getSqlStatement(table, toFlush.get(0));
            flushBufferStopwatch.start();
            recordWriter.write(toFlush, sqlStatement);
            flushBufferStopwatch.stop();

            LOGGER.trace("[PERF] Flush buffer execution time {}", flushBufferStopwatch.durations());
            LOGGER.trace("[PERF] Table changes execution time {}", tableChangesStopwatch.durations());
        }
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

    private TableDescriptor checkAndApplyTableChangesIfNeeded(CollectionId collectionId, JdbcSinkRecord record) throws SQLException {
        if (!hasTable(collectionId)) {
            // Table does not exist, lets attempt to create it.
            try {
                return createTable(collectionId, record);
            }
            catch (SQLException ce) {
                // It's possible the table may have been created in the interim, so try to alter.
                LOGGER.warn("Table creation failed for '{}', attempting to alter the table", collectionId.toFullIdentiferString(), ce);
                try {
                    return alterTableIfNeeded(collectionId, record);
                }
                catch (SQLException ae) {
                    // The alter failed, hard stop.
                    LOGGER.error("Failed to alter the table '{}'.", collectionId.toFullIdentiferString(), ae);
                    throw ae;
                }
            }
        }
        else {
            // Table exists, lets attempt to alter it if necessary.
            try {
                return alterTableIfNeeded(collectionId, record);
            }
            catch (SQLException ae) {
                LOGGER.error("Failed to alter the table '{}'.", collectionId.toFullIdentiferString(), ae);
                throw ae;
            }
        }
    }

    private boolean hasTable(CollectionId collectionId) {
        return session.doReturningWork((connection) -> dialect.tableExists(connection, collectionId));
    }

    private TableDescriptor readTable(CollectionId collectionId) {
        return session.doReturningWork((connection) -> dialect.readTable(connection, collectionId));
    }

    private TableDescriptor createTable(CollectionId collectionId, JdbcSinkRecord record) throws SQLException {
        LOGGER.debug("Attempting to create table '{}'.", collectionId.toFullIdentiferString());

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be created because schema evolution is disabled.", collectionId.toFullIdentiferString());
            throw new SQLException("Cannot create table " + collectionId.toFullIdentiferString() + " because schema evolution is disabled");
        }

        Transaction transaction = session.beginTransaction();
        try {
            final String createSql = dialect.getCreateTableStatement(record, collectionId);
            LOGGER.trace("SQL: {}", createSql);
            session.createNativeQuery(createSql, Object.class).executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }

        return readTable(collectionId);
    }

    private TableDescriptor alterTableIfNeeded(CollectionId collectionId, JdbcSinkRecord record) throws SQLException {
        LOGGER.debug("Attempting to alter table '{}'.", collectionId.toFullIdentiferString());

        if (!hasTable(collectionId)) {
            LOGGER.error("Table '{}' does not exist and cannot be altered.", collectionId.toFullIdentiferString());
            throw new SQLException("Could not find table: " + collectionId.toFullIdentiferString());
        }

        // Resolve table metadata from the database
        final TableDescriptor table = readTable(collectionId);

        // Delegating to dialect to deal with database case sensitivity.
        Set<String> missingFields = dialect.resolveMissingFields(record, table);
        if (missingFields.isEmpty()) {
            // There are no missing fields, simply return
            // todo: should we check column type changes or default value changes?
            return table;
        }

        LOGGER.debug("The follow fields are missing in the table: {}", missingFields);
        for (String missingFieldName : missingFields) {
            final FieldDescriptor fieldDescriptor = record.allFields().get(missingFieldName);
            if (!fieldDescriptor.getSchema().isOptional() && fieldDescriptor.getSchema().defaultValue() == null) {
                throw new SQLException(String.format(
                        "Cannot ALTER table '%s' because field '%s' is not optional but has no default value",
                        collectionId.toFullIdentiferString(), fieldDescriptor.getName()));
            }
        }

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be altered because schema evolution is disabled.", collectionId.toFullIdentiferString());
            throw new SQLException("Cannot alter table " + collectionId.toFullIdentiferString() + " because schema evolution is disabled");
        }

        Transaction transaction = session.beginTransaction();
        try {
            final String alterSql = dialect.getAlterTableStatement(table, record, missingFields);
            LOGGER.trace("SQL: {}", alterSql);
            session.createNativeQuery(alterSql, Object.class).executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }

        return readTable(collectionId);
    }

    private String getSqlStatement(TableDescriptor table, JdbcSinkRecord record) {
        if (!record.isDelete()) {
            switch (config.getInsertMode()) {
                case INSERT:
                    return dialect.getInsertStatement(table, record);
                case UPSERT:
                    if (record.keyFieldNames().isEmpty()) {
                        throw new ConnectException("Cannot write to table " + table.getId().name() + " with no key fields defined.");
                    }
                    return dialect.getUpsertStatement(table, record);
                case UPDATE:
                    return dialect.getUpdateStatement(table, record);
            }
        }
        else {
            return dialect.getDeleteStatement(table, record);
        }

        throw new DataException(String.format("Unable to get SQL statement for %s", record));
    }

    private void writeTruncate(String sql) throws SQLException {
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);

            query.executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }

    public Optional<CollectionId> getCollectionId(String collectionName) {
        return Optional.of(dialect.getCollectionId(collectionName));
    }

    private boolean isRetriable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        for (Class<? extends Exception> e : dialect.getCommunicationExceptions()) {
            if (e.isAssignableFrom(throwable.getClass())) {
                return true;
            }
        }
        return isRetriable(throwable.getCause());
    }

    public Optional<CollectionId> getCollectionIdFromRecord(DebeziumSinkRecord record) {
        String tableName = this.config.getCollectionNamingStrategy().resolveCollectionName(record, config.getCollectionNameFormat());
        if (tableName == null) {
            return Optional.empty();
        }
        return getCollectionId(tableName);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE;

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

import io.debezium.connector.jdbc.SinkRecordDescriptor.FieldDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.spi.ChangeEventSink;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;
import io.debezium.util.Strings;

/**
 * A {@link ChangeEventSink} for a JDBC relational database.
 *
 * @author Chris Cranford
 */
public class JdbcChangeEventSink implements ChangeEventSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcChangeEventSink.class);

    public static final String SCHEMA_CHANGE_VALUE = "SchemaChangeValue";
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

    @Override
    public void execute(Collection<SinkRecord> records) {
        final Map<CollectionId, Buffer> upsertBufferByTable = new LinkedHashMap<>();
        final Map<CollectionId, Buffer> deleteBufferByTable = new LinkedHashMap<>();

        for (SinkRecord record : records) {

            LOGGER.trace("Processing {}", record);

            validate(record);

            Optional<CollectionId> optionalCollectionId = getCollectionIdFromRecord(record);
            if (optionalCollectionId.isEmpty()) {

                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'. No resolvable table name", record.topic(), record.kafkaPartition(),
                        record.kafkaOffset());
                continue;
            }

            SinkRecordDescriptor sinkRecordDescriptor = buildRecordSinkDescriptor(record);

            if (sinkRecordDescriptor.isTombstone()) {
                // Skip only Debezium Envelope tombstone not the one produced by ExtractNewRecordState SMT
                LOGGER.debug("Skipping tombstone record {}", sinkRecordDescriptor);
                continue;
            }

            final CollectionId collectionId = optionalCollectionId.get();

            if (sinkRecordDescriptor.isTruncate()) {

                if (!config.isTruncateEnabled()) {
                    LOGGER.debug("Truncates are not enabled, skipping truncate for topic '{}'", sinkRecordDescriptor.getTopicName());
                    continue;
                }

                // Here we want to flush the buffer to let truncate having effect on the buffered events.
                flushBuffers(upsertBufferByTable);
                flushBuffers(deleteBufferByTable);

                try {
                    final TableDescriptor table = checkAndApplyTableChangesIfNeeded(collectionId, sinkRecordDescriptor);
                    writeTruncate(dialect.getTruncateStatement(table));
                    continue;
                }
                catch (SQLException e) {
                    throw new ConnectException("Failed to process a sink record", e);
                }
            }

            if (sinkRecordDescriptor.isDelete()) {

                if (!config.isDeleteEnabled()) {
                    LOGGER.debug("Deletes are not enabled, skipping delete for topic '{}'", sinkRecordDescriptor.getTopicName());
                    continue;
                }

                if (upsertBufferByTable.get(collectionId) != null && !upsertBufferByTable.get(collectionId).isEmpty()) {
                    // When a delete event arrives, update buffer must be flushed to avoid losing the delete
                    // for the same record after its update.

                    flushBufferWithRetries(collectionId, upsertBufferByTable.get(collectionId).flush());
                }

                Buffer tableIdBuffer = resolveBuffer(deleteBufferByTable, collectionId, sinkRecordDescriptor);

                List<SinkRecordDescriptor> toFlush = tableIdBuffer.add(sinkRecordDescriptor);

                flushBufferWithRetries(collectionId, toFlush);
            }
            else {
                if (deleteBufferByTable.get(collectionId) != null && !deleteBufferByTable.get(collectionId).isEmpty()) {
                    // When an insert arrives, delete buffer must be flushed to avoid losing an insert for the same record after its deletion.
                    // this because at the end we will always flush inserts before deletes.
                    flushBufferWithRetries(collectionId, deleteBufferByTable.get(collectionId).flush());
                }

                Stopwatch updateBufferStopwatch = Stopwatch.reusable();
                updateBufferStopwatch.start();

                Buffer tableIdBuffer = resolveBuffer(upsertBufferByTable, collectionId, sinkRecordDescriptor);

                List<SinkRecordDescriptor> toFlush = tableIdBuffer.add(sinkRecordDescriptor);
                updateBufferStopwatch.stop();

                LOGGER.trace("[PERF] Update buffer execution time {}", updateBufferStopwatch.durations());
                flushBufferWithRetries(collectionId, toFlush);
            }

        }

        flushBuffers(upsertBufferByTable);
        flushBuffers(deleteBufferByTable);
    }

    private void validate(SinkRecord record) {

        if (isSchemaChange(record)) {
            LOGGER.error(DETECT_SCHEMA_CHANGE_RECORD_MSG);
            throw new DataException(DETECT_SCHEMA_CHANGE_RECORD_MSG);
        }
    }

    private static boolean isSchemaChange(SinkRecord record) {
        return record.valueSchema() != null
                && !Strings.isNullOrEmpty(record.valueSchema().name())
                && record.valueSchema().name().contains(SCHEMA_CHANGE_VALUE);
    }

    private Buffer resolveBuffer(Map<CollectionId, Buffer> bufferMap, CollectionId collectionId, SinkRecordDescriptor sinkRecordDescriptor) {
        if (config.isUseReductionBuffer() && !sinkRecordDescriptor.getKeyFieldNames().isEmpty()) {
            return bufferMap.computeIfAbsent(collectionId, k -> new ReducedRecordBuffer(config));
        }
        else {
            return bufferMap.computeIfAbsent(collectionId, k -> new RecordBuffer(config));
        }
    }

    private SinkRecordDescriptor buildRecordSinkDescriptor(SinkRecord record) {

        SinkRecordDescriptor sinkRecordDescriptor;
        try {
            sinkRecordDescriptor = SinkRecordDescriptor.builder()
                    .withPrimaryKeyMode(config.getPrimaryKeyMode())
                    .withPrimaryKeyFields(config.getPrimaryKeyFields())
                    .withFieldFilter(config.getFieldFilter())
                    .withSinkRecord(record)
                    .withDialect(dialect)
                    .build();
        }
        catch (Exception e) {
            throw new ConnectException("Failed to process a sink record", e);
        }
        return sinkRecordDescriptor;
    }

    private void flushBuffers(Map<CollectionId, Buffer> bufferByTable) {
        bufferByTable.forEach((collectionId, recordBuffer) -> flushBufferWithRetries(collectionId, recordBuffer.flush()));
    }

    private void flushBufferWithRetries(CollectionId collectionId, List<SinkRecordDescriptor> toFlush) {
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
                flushBuffer(collectionId, toFlush);
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

    private void flushBuffer(CollectionId collectionId, List<SinkRecordDescriptor> toFlush) throws SQLException {
        Stopwatch flushBufferStopwatch = Stopwatch.reusable();
        Stopwatch tableChangesStopwatch = Stopwatch.reusable();
        if (!toFlush.isEmpty()) {
            LOGGER.debug("Flushing records in JDBC Writer for table: {}", collectionId.name());
            tableChangesStopwatch.start();
            final TableDescriptor table = checkAndApplyTableChangesIfNeeded(collectionId, toFlush.get(0));
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

    private TableDescriptor checkAndApplyTableChangesIfNeeded(CollectionId collectionId, SinkRecordDescriptor descriptor) throws SQLException {
        if (!hasTable(collectionId)) {
            // Table does not exist, lets attempt to create it.
            try {
                return createTable(collectionId, descriptor);
            }
            catch (SQLException ce) {
                // It's possible the table may have been created in the interim, so try to alter.
                LOGGER.warn("Table creation failed for '{}', attempting to alter the table", collectionId.toFullIdentiferString(), ce);
                try {
                    return alterTableIfNeeded(collectionId, descriptor);
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
                return alterTableIfNeeded(collectionId, descriptor);
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

    private TableDescriptor createTable(CollectionId collectionId, SinkRecordDescriptor record) throws SQLException {
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

    private TableDescriptor alterTableIfNeeded(CollectionId collectionId, SinkRecordDescriptor record) throws SQLException {
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
            final FieldDescriptor fieldDescriptor = record.getFields().get(missingFieldName);
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

    private String getSqlStatement(TableDescriptor table, SinkRecordDescriptor record) {

        if (!record.isDelete()) {
            switch (config.getInsertMode()) {
                case INSERT:
                    return dialect.getInsertStatement(table, record);
                case UPSERT:
                    if (record.getKeyFieldNames().isEmpty()) {
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

    public Optional<CollectionId> getCollectionIdFromRecord(SinkRecord record) {
        String tableName = this.config.getCollectionNamingStrategy().resolveCollectionName(config, record);
        if (tableName == null) {
            return Optional.empty();
        }
        return getCollectionId(tableName);
    }
}

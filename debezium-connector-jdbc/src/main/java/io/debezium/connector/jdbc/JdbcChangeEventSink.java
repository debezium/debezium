/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.SinkRecordDescriptor.FieldDescriptor;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectResolver;
import io.debezium.connector.jdbc.naming.TableNamingStrategy;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.relational.TableId;
import io.debezium.pipeline.sink.spi.ChangeEventSink;

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
    private final SessionFactory sessionFactory;
    private final DatabaseDialect dialect;
    private final StatelessSession session;
    private final TableNamingStrategy tableNamingStrategy;
    private QueryBinderResolver queryBinderResolver;

    public JdbcChangeEventSink(JdbcSinkConnectorConfig config) {
        this.config = config;
        this.sessionFactory = config.getHibernateConfiguration().buildSessionFactory();
        this.tableNamingStrategy = config.getTableNamingStrategy();

        this.dialect = DatabaseDialectResolver.resolve(config, sessionFactory);
        this.session = this.sessionFactory.openStatelessSession();
        this.queryBinderResolver = new QueryBinderResolver(); // TODO maybe move it upper fot testability
        final DatabaseVersion version = this.dialect.getVersion();
        LOGGER.info("Database version {}.{}.{}", version.getMajor(), version.getMinor(), version.getMicro());
    }

    @Override
    public void execute(SinkRecord record) {
        try {
            // Examine the sink record and prepare a descriptor
            final SinkRecordDescriptor descriptor = SinkRecordDescriptor.builder()
                    .withPrimaryKeyMode(config.getPrimaryKeyMode())
                    .withPrimaryKeyFields(config.getPrimaryKeyFields())
                    .withSinkRecord(record)
                    .withDialect(dialect)
                    .build();

            if (descriptor.isTombstone()) {
                LOGGER.debug("Skipping tombstone record {}", descriptor);
                return;
            }

            String tableName = tableNamingStrategy.resolveTableName(config, record);
            if (tableName == null) {
                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'", record.topic(), record.kafkaPartition(), record.kafkaOffset());
                return;
            }
            final TableId tableId = dialect.getTableId(tableName);
            final TableDescriptor table = checkAndApplyTableChangesIfNeeded(tableId, descriptor);
            write(table, descriptor);
        }
        catch (Exception e) {
            throw new ConnectException("Failed to process a sink record", e);
        }
    }

    @Override
    public void execute(Collection<SinkRecord> records) {

        final Map<TableId, RecordBuffer> updateBufferByTable = new HashMap<>();
        final Map<TableId, RecordBuffer> deleteBufferByTable = new HashMap<>();

        for (SinkRecord record : records) {

            LOGGER.trace("Processing {}", record);

            validate(record);

            Optional<TableId> optionalTableId = getTableId(record);
            if (optionalTableId.isEmpty()) {

                LOGGER.warn("Ignored to write record from topic '{}' partition '{}' offset '{}'. No resolvable table name", record.topic(), record.kafkaPartition(),
                        record.kafkaOffset());
                continue;
            }

            SinkRecordDescriptor sinkRecordDescriptor = buildRecordSinkDescriptor(record);

            final TableId tableId = optionalTableId.get();

            if (sinkRecordDescriptor.isTombstone()) {
                LOGGER.debug("Skipping tombstone record {}", sinkRecordDescriptor);
                continue;
            }

            if (sinkRecordDescriptor.isTruncate()) {

                if (!config.isTruncateEnabled()) {
                    LOGGER.debug("Truncates are not enabled, skipping truncate for topic '{}'", sinkRecordDescriptor.getTopicName());
                    continue;
                }

                // Here we want to flush the buffer to let truncate having effect on the buffered events.
                flushBuffers(updateBufferByTable);

                flushBuffers(deleteBufferByTable);

                try {
                    final TableDescriptor table = checkAndApplyTableChangesIfNeeded(tableId, sinkRecordDescriptor);
                    writeTruncate(dialect.getTruncateStatement(table));
                }
                catch (SQLException e) {
                    throw new ConnectException("Failed to process a sink record", e);
                }
            }

            if (sinkRecordDescriptor.isDelete()) {

                if (!config.isDeleteEnabled()) {
                    LOGGER.debug("Deletes are not enabled, skipping delete for topic '{}'", sinkRecordDescriptor.getTopicName());
                    try { // TODO Do we want to maintain this behavior? Should table be created on delete only when deletes are enabled?
                        checkAndApplyTableChangesIfNeeded(tableId, sinkRecordDescriptor);
                    }
                    catch (Exception e) {
                        throw new ConnectException("Failed to process a sink record", e);
                    }
                    continue;
                }

                RecordBuffer tableIdBuffer = deleteBufferByTable.computeIfAbsent(tableId, k -> new RecordBuffer(config));
                List<SinkRecordDescriptor> toFlush = tableIdBuffer.add(sinkRecordDescriptor);

                flushBuffer(tableId, toFlush);
            }
            else {

                if (deleteBufferByTable.get(tableId) != null && !deleteBufferByTable.get(tableId).isEmpty()) {
                    // When an insert arrives, delete buffer must be flushed to avoid losing an insert for the same record after its deletion.
                    // this because at the end we will always flush inserts before deletes.

                    flushBuffer(tableId, deleteBufferByTable.get(tableId).flush());
                }

                RecordBuffer tableIdBuffer = updateBufferByTable.computeIfAbsent(tableId, k -> new RecordBuffer(config));

                List<SinkRecordDescriptor> toFlush = tableIdBuffer.add(sinkRecordDescriptor);
                flushBuffer(tableId, toFlush);
            }

        }

        flushBuffers(updateBufferByTable);

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

    private SinkRecordDescriptor buildRecordSinkDescriptor(SinkRecord record) {
        SinkRecordDescriptor sinkRecordDescriptor;
        try {
            sinkRecordDescriptor = SinkRecordDescriptor.builder()
                    .withPrimaryKeyMode(config.getPrimaryKeyMode())
                    .withPrimaryKeyFields(config.getPrimaryKeyFields())
                    .withSinkRecord(record)
                    .withDialect(dialect)
                    .build();
        }
        catch (Exception e) {
            throw new ConnectException("Failed to process a sink record", e);
        }
        return sinkRecordDescriptor;
    }

    private void flushBuffers(Map<TableId, RecordBuffer> bufferByTable) {

        bufferByTable.forEach((tableId, recordBuffer) -> flushBuffer(tableId, recordBuffer.flush()));
    }

    private void flushBuffer(TableId tableId, List<SinkRecordDescriptor> toFlush) {

        if (!toFlush.isEmpty()) {
            LOGGER.debug("Flushing records in JDBC Writer for table: {}", tableId.getTableName());
            writeBuffer(tableId, toFlush);
        }

    }

    private void writeBuffer(TableId tableId, List<SinkRecordDescriptor> records) {

        final String sqlStatement;
        try {

            final TableDescriptor table = checkAndApplyTableChangesIfNeeded(tableId, records.get(0));
            sqlStatement = getSqlStatement(table, records.get(0));
        }
        catch (Exception e) {
            throw new ConnectException("Failed to process a sink record", e);
        }

        final Transaction transaction = session.beginTransaction();
        try {
            session.doWork(conn -> {

                try (PreparedStatement pstmt = conn.prepareStatement(sqlStatement)) {

                    QueryBinder queryBinder = queryBinderResolver.resolve(pstmt);
                    for (SinkRecordDescriptor sinkRecordDescriptor : records) {

                        bindValues(sinkRecordDescriptor, queryBinder);

                        pstmt.addBatch();
                    }

                    int[] batchResult = pstmt.executeBatch();
                    for (int updateCount : batchResult) {
                        if (updateCount == Statement.EXECUTE_FAILED) {
                            throw new BatchUpdateException(
                                    "Execution failed for part of the batch", batchResult);
                        }
                    }
                }
            });
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }

    private Optional<TableId> getTableId(SinkRecord record) {

        String tableName = tableNamingStrategy.resolveTableName(config, record);
        if (tableName == null) {
            return Optional.empty();
        }

        return Optional.of(dialect.getTableId(tableName));
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

        if (sessionFactory != null && sessionFactory.isOpen()) {
            LOGGER.info("Closing the session factory");
            sessionFactory.close();
        }
        else {
            LOGGER.info("Session factory already closed");
        }
    }

    private TableDescriptor checkAndApplyTableChangesIfNeeded(TableId tableId, SinkRecordDescriptor descriptor) throws SQLException {
        if (!hasTable(tableId)) {
            // Table does not exist, lets attempt to create it.
            try {
                return createTable(tableId, descriptor);
            }
            catch (SQLException ce) {
                // It's possible the table may have been created in the interim, so try to alter.
                LOGGER.warn("Table creation failed for '{}', attempting to alter the table", tableId.toFullIdentiferString(), ce);
                try {
                    return alterTableIfNeeded(tableId, descriptor);
                }
                catch (SQLException ae) {
                    // The alter failed, hard stop.
                    LOGGER.error("Failed to alter the table '{}'.", tableId.toFullIdentiferString(), ae);
                    throw ae;
                }
            }
        }
        else {
            // Table exists, lets attempt to alter it if necessary.
            try {
                return alterTableIfNeeded(tableId, descriptor);
            }
            catch (SQLException ae) {
                LOGGER.error("Failed to alter the table '{}'.", tableId.toFullIdentiferString(), ae);
                throw ae;
            }
        }
    }

    private boolean hasTable(TableId tableId) {
        return session.doReturningWork((connection) -> dialect.tableExists(connection, tableId));
    }

    private TableDescriptor readTable(TableId tableId) {
        return session.doReturningWork((connection) -> dialect.readTable(connection, tableId));
    }

    private TableDescriptor createTable(TableId tableId, SinkRecordDescriptor record) throws SQLException {
        LOGGER.debug("Attempting to create table '{}'.", tableId.toFullIdentiferString());

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be created because schema evolution is disabled.", tableId.toFullIdentiferString());
            throw new SQLException("Cannot create table " + tableId.toFullIdentiferString() + " because schema evolution is disabled");
        }

        Transaction transaction = session.beginTransaction();
        try {
            final String createSql = dialect.getCreateTableStatement(record, tableId);
            LOGGER.trace("SQL: {}", createSql);
            session.createNativeQuery(createSql, Object.class).executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }

        return readTable(tableId);
    }

    private TableDescriptor alterTableIfNeeded(TableId tableId, SinkRecordDescriptor record) throws SQLException {
        LOGGER.debug("Attempting to alter table '{}'.", tableId.toFullIdentiferString());

        if (!hasTable(tableId)) {
            LOGGER.error("Table '{}' does not exist and cannot be altered.", tableId.toFullIdentiferString());
            throw new SQLException("Could not find table: " + tableId.toFullIdentiferString());
        }

        // Resolve table metadata from the database
        final TableDescriptor table = readTable(tableId);

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
                        tableId.toFullIdentiferString(), fieldDescriptor.getName()));
            }
        }

        if (NONE.equals(config.getSchemaEvolutionMode())) {
            LOGGER.warn("Table '{}' cannot be altered because schema evolution is disabled.", tableId.toFullIdentiferString());
            throw new SQLException("Cannot alter table " + tableId.toFullIdentiferString() + " because schema evolution is disabled");
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

        return readTable(tableId);
    }

    private void write(TableDescriptor table, SinkRecordDescriptor record) throws SQLException {
        if (record.isDelete()) {
            writeDelete(dialect.getDeleteStatement(table, record), record);
        }
        else if (record.isTruncate()) {
            writeTruncate(dialect.getTruncateStatement(table));
        }
        else {
            switch (config.getInsertMode()) {
                case INSERT:
                    writeInsert(dialect.getInsertStatement(table, record), record);
                    break;
                case UPSERT:
                    if (record.getKeyFieldNames().isEmpty()) {
                        throw new ConnectException("Cannot write to table " + table.getId().getTableName() + " with no key fields defined.");
                    }
                    writeUpsert(dialect.getUpsertStatement(table, record), record);
                    break;
                case UPDATE:
                    writeUpdate(dialect.getUpdateStatement(table, record), record);
                    break;
            }
        }
    }

    private void writeInsert(String sql, SinkRecordDescriptor record) throws SQLException {
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);
            QueryBinder queryBinder = queryBinderResolver.resolve(query);
            int index = bindKeyValuesToQuery(record, queryBinder, 1);
            bindNonKeyValuesToQuery(record, queryBinder, index);

            final int result = query.executeUpdate();
            if (result != 1) {
                throw new SQLException("Failed to insert row from table");
            }

            transaction.commit();
        }
        catch (SQLException e) {
            transaction.rollback();
            throw e;
        }
    }

    private void bindValues(SinkRecordDescriptor sinkRecordDescriptor, QueryBinder queryBinder) {

        int index;
        if (sinkRecordDescriptor.isDelete()) {
            bindKeyValuesToQuery(sinkRecordDescriptor, queryBinder, 1);
            return;
        }

        switch (config.getInsertMode()) {
            case INSERT:
            case UPSERT:
                index = bindKeyValuesToQuery(sinkRecordDescriptor, queryBinder, 1);
                bindNonKeyValuesToQuery(sinkRecordDescriptor, queryBinder, index);
                break;
            case UPDATE:
                index = bindNonKeyValuesToQuery(sinkRecordDescriptor, queryBinder, 1);
                bindKeyValuesToQuery(sinkRecordDescriptor, queryBinder, index);
                break;
        }

    }

    private String getSqlStatement(TableDescriptor table, SinkRecordDescriptor record) {

        if (!record.isDelete()) {
            switch (config.getInsertMode()) {
                case INSERT:
                    return dialect.getInsertStatement(table, record);
                case UPSERT:
                    if (record.getKeyFieldNames().isEmpty()) {
                        throw new ConnectException("Cannot write to table " + table.getId().getTableName() + " with no key fields defined.");
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

    private void writeUpsert(String sql, SinkRecordDescriptor record) throws SQLException {
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);
            QueryBinder queryBinder = queryBinderResolver.resolve(query);
            int index = bindKeyValuesToQuery(record, queryBinder, 1);
            bindNonKeyValuesToQuery(record, queryBinder, index);

            query.executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }

    private void writeUpdate(String sql, SinkRecordDescriptor record) throws SQLException {
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);
            QueryBinder queryBinder = queryBinderResolver.resolve(query);
            int index = bindNonKeyValuesToQuery(record, queryBinder, 1);
            bindKeyValuesToQuery(record, queryBinder, index);

            query.executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }

    private void writeDelete(String sql, SinkRecordDescriptor record) {
        if (!config.isDeleteEnabled()) {
            LOGGER.debug("Deletes are not enabled, skipping delete for topic '{}'", record.getTopicName());
            return;
        }
        final Transaction transaction = session.beginTransaction();
        try {
            LOGGER.trace("SQL: {}", sql);
            final NativeQuery<?> query = session.createNativeQuery(sql, Object.class);
            bindKeyValuesToQuery(record, queryBinderResolver.resolve(query), 1);

            query.executeUpdate();
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
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

    private int bindKeyValuesToQuery(SinkRecordDescriptor record, QueryBinder query, int index) {

        if (Objects.requireNonNull(config.getPrimaryKeyMode()) == JdbcSinkConnectorConfig.PrimaryKeyMode.KAFKA) {
            query.bind(new ValueBindDescriptor(index++, record.getTopicName()));
            query.bind(new ValueBindDescriptor(index++, record.getPartition()));
            query.bind(new ValueBindDescriptor(index++, record.getOffset()));
        }
        else {
            final Struct keySource = record.getKeyStruct(config.getPrimaryKeyMode());
            if (keySource != null) {
                index = bindFieldValuesToQuery(record, query, index, keySource, record.getKeyFieldNames());
            }
        }
        return index;
    }

    private int bindNonKeyValuesToQuery(SinkRecordDescriptor record, QueryBinder query, int index) {
        return bindFieldValuesToQuery(record, query, index, record.getAfterStruct(), record.getNonKeyFieldNames());
    }

    private int bindFieldValuesToQuery(SinkRecordDescriptor record, QueryBinder query, int index, Struct source, List<String> fields) {
        for (String fieldName : fields) {
            final FieldDescriptor field = record.getFields().get(fieldName);
            List<ValueBindDescriptor> boundValues = dialect.bindValue(field, index, source.get(fieldName));

            boundValues.forEach(query::bind);
            index += boundValues.size();
        }
        return index;
    }

}

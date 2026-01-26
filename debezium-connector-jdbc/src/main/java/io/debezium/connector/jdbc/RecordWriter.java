/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.JdbcSinkConnectorConfig.SchemaEvolutionMode.NONE;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.JDBCException;
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.hibernate.query.NativeQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.batch.Batch;
import io.debezium.sink.batch.BatchRecord;
import io.debezium.sink.field.FieldDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.util.Stopwatch;

/**
 * Effectively writes the batches using Hibernate {@link Work}
 *
 * @author Mario Fiore Vitale
 */
public class RecordWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWriter.class);
    private final SharedSessionContract session;
    private final QueryBinderResolver queryBinderResolver;
    private final JdbcSinkConnectorConfig config;
    private final DatabaseDialect dialect;

    public RecordWriter(SharedSessionContract session, QueryBinderResolver queryBinderResolver, JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
        this.session = session;
        this.queryBinderResolver = queryBinderResolver;
        this.config = config;
        this.dialect = dialect;
    }

    private String getSqlStatement(TableDescriptor table, JdbcSinkRecord record) {
        if (!record.isDelete()) {
            return switch (config.getInsertMode()) {
                case INSERT -> dialect.getInsertStatement(table, record);
                case UPSERT -> {
                    if (record.keyFieldNames().isEmpty()) {
                        throw new ConnectException("Cannot write to table " + table.getId().name() + " with no key fields defined.");
                    }
                    yield dialect.getUpsertStatement(table, record);
                }
                case UPDATE -> dialect.getUpdateStatement(table, record);
            };
        }
        else {
            return dialect.getDeleteStatement(table, record);
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

    public TableDescriptor checkAndApplyTableChangesIfNeeded(CollectionId collectionId, JdbcSinkRecord record) throws SQLException {
        if (!hasTable(collectionId)) {
            // Table does not exist, lets attempt to create it.
            try {
                return createTable(collectionId, record);
            }
            catch (SQLException | JDBCException ce) {
                // It's possible the table may have been created in the interim, so try to alter.
                LOGGER.warn("Table creation failed for '{}', attempting to alter the table", collectionId.toFullIdentiferString(), ce);
                try {
                    return alterTableIfNeeded(collectionId, record);
                }
                catch (SQLException | JDBCException ae) {
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
            catch (SQLException | JDBCException ae) {
                LOGGER.error("Failed to alter the table '{}'.", collectionId.toFullIdentiferString(), ae);
                throw ae;
            }
        }
    }

    public void write(Batch records) {
        Stopwatch writeStopwatch = Stopwatch.reusable();
        writeStopwatch.start();
        final Transaction transaction = session.beginTransaction();

        try {
            session.doWork(processBatch(records));
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
        writeStopwatch.stop();
        LOGGER.trace("[PERF] Total write execution time {}", writeStopwatch.durations());
    }

    public void write(List<JdbcSinkRecord> records, TableDescriptor table) {
        Stopwatch writeStopwatch = Stopwatch.reusable();
        writeStopwatch.start();
        final Transaction transaction = session.beginTransaction();

        try {
            session.doWork(processBatch(records, table));
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
        writeStopwatch.stop();
        LOGGER.trace("[PERF] Total write execution time {}", writeStopwatch.durations());
    }

    public void writeTruncate(CollectionId collectionId) throws SQLException {
        final Transaction transaction = session.beginTransaction();
        try {
            var sql = dialect.getTruncateStatement(collectionId);
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

    private Work processBatch(List<JdbcSinkRecord> records, TableDescriptor table) {
        return conn -> {
            performWrite(conn, table, records);
        };
    }

    private void performWrite(Connection conn, TableDescriptor table, List<JdbcSinkRecord> records) throws SQLException {
        try (PreparedStatement prepareStatement = conn.prepareStatement(getSqlStatement(table, records.get(0)))) {
            QueryBinder queryBinder = queryBinderResolver.resolve(prepareStatement);
            Stopwatch allbindStopwatch = Stopwatch.reusable();
            allbindStopwatch.start();
            for (JdbcSinkRecord record : records) {

                Stopwatch singlebindStopwatch = Stopwatch.reusable();
                singlebindStopwatch.start();
                bindValues(record, queryBinder);
                singlebindStopwatch.stop();

                Stopwatch addBatchStopwatch = Stopwatch.reusable();
                addBatchStopwatch.start();
                prepareStatement.addBatch();
                addBatchStopwatch.stop();

                LOGGER.trace("[PERF] Bind single record execution time {}", singlebindStopwatch.durations());
                LOGGER.trace("[PERF] Add batch execution time {}", addBatchStopwatch.durations());
            }
            allbindStopwatch.stop();
            LOGGER.trace("[PERF] All records bind execution time {}", allbindStopwatch.durations());

            Stopwatch executeStopwatch = Stopwatch.reusable();
            executeStopwatch.start();
            int[] batchResult = prepareStatement.executeBatch();
            executeStopwatch.stop();
            for (int updateCount : batchResult) {
                if (updateCount == Statement.EXECUTE_FAILED) {
                    throw new BatchUpdateException("Execution failed for part of the batch", batchResult);
                }
            }
            LOGGER.trace("[PERF] Execute batch execution time {}", executeStopwatch.durations());
        }
    }

    private record WriteRecord(TableDescriptor table, DebeziumSinkRecord record) {
    }

    private Work processBatch(Batch records) {
        return conn -> {

            Map<TableDescriptor, List<JdbcSinkRecord>> insertsByTable = new LinkedHashMap<>();
            Map<TableDescriptor, List<JdbcSinkRecord>> deletesByTable = new LinkedHashMap<>();

            for (BatchRecord batchRecord : records) {

                var record = (JdbcKafkaSinkRecord) batchRecord.record();
                var collectionId = batchRecord.collectionId();

                var table = checkAndApplyTableChangesIfNeeded(collectionId, record);

                if (record.isTruncate()) {
                    // if (!insertsByTable.isEmpty()) {
                    // performWrite(conn, table, insertsByTable.get(table.getId()));
                    // }
                    // if (!deletesByTable.isEmpty()) {
                    // performWrite(conn, table, deletesByTable.get(table.getId()));
                    // }
                    writeTruncate(collectionId);
                }
                else if (record.isDelete()) {
                    deletesByTable.computeIfAbsent(table, k -> new ArrayList<>()).add(record);
                }
                else {
                    insertsByTable.computeIfAbsent(table, k -> new ArrayList<>()).add(record);
                }
            }

            for (Map.Entry<TableDescriptor, List<JdbcSinkRecord>> deleteEntry : deletesByTable.entrySet()) {
                performWrite(conn, deleteEntry.getKey(), deleteEntry.getValue());
            }
            for (Map.Entry<TableDescriptor, List<JdbcSinkRecord>> insertsEntry : insertsByTable.entrySet()) {
                performWrite(conn, insertsEntry.getKey(), insertsEntry.getValue());
            }
        };
    }

    private void bindValues(JdbcSinkRecord record, QueryBinder queryBinder) {
        int index;
        if (record.isDelete()) {
            bindKeyValuesToQuery(record, queryBinder, 1);
            return;
        }

        switch (config.getInsertMode()) {
            case INSERT:
            case UPSERT:
                index = bindKeyValuesToQuery(record, queryBinder, 1);
                bindNonKeyValuesToQuery(record, queryBinder, index);
                break;
            case UPDATE:
                index = bindNonKeyValuesToQuery(record, queryBinder, 1);
                bindKeyValuesToQuery(record, queryBinder, index);
                break;
        }
    }

    private int bindKeyValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index) {
        final Struct keySource = record.filteredKey();
        if (keySource != null) {
            index = bindFieldValuesToQuery(record, query, index, keySource, record.keyFieldNames());
        }
        return index;
    }

    private int bindNonKeyValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index) {
        return bindFieldValuesToQuery(record, query, index, record.getPayload(), record.nonKeyFieldNames());
    }

    private int bindFieldValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index, Struct source, Set<String> fieldNames) {
        for (String fieldName : fieldNames) {
            final JdbcFieldDescriptor field = record.jdbcFields().get(fieldName);

            Object value;
            if (field.getSchema().isOptional()) {
                value = source.getWithoutDefault(fieldName);
            }
            else {
                value = source.get(fieldName);
            }
            List<ValueBindDescriptor> boundValues = dialect.bindValue(field, index, value);

            boundValues.forEach(query::bind);
            index += boundValues.size();
        }
        return index;
    }
}

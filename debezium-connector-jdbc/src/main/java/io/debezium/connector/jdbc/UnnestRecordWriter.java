/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SharedSessionContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.BinaryHandling;
import io.debezium.sink.spi.SinkProgressListener;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.util.Stopwatch;

/**
 * UNNEST-optimized implementation for PostgreSQL-compatible targets that writes batches using SQL arrays.
 * This approach can provide 5-10x performance improvement for bulk inserts/upserts.
 *
 * For batch statements (isBatchStatement=true), uses UNNEST with PreparedStatement.setArray().
 * This ensures a single SQL query plan regardless of batch size, eliminating the query plan
 * explosion problem in pg_stat_statements.
 *
 * For non-batch statements, delegates to parent's standard row-wise binding.
 *
 * @author Gaurav Miglani
 */
public class UnnestRecordWriter extends DefaultRecordWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnnestRecordWriter.class);

    public UnnestRecordWriter(SharedSessionContract session, QueryBinderResolver queryBinderResolver,
                              JdbcSinkConnectorConfig config, DatabaseDialect dialect, SinkProgressListener progressListener) {
        super(session, queryBinderResolver, config, dialect, progressListener);
    }

    @Override
    protected void performTableWrite(Connection conn, TableDescriptor table, List<JdbcSinkRecord> records) throws SQLException {
        SqlStatementInfo statementInfo = getSqlStatementInfo(table, records);
        if (statementInfo.isBatchStatement()) {
            performUnnestBatch(conn, table, statementInfo.statement(), records);
        }
        else {
            super.performTableWrite(conn, table, records);
        }
    }

    void performUnnestBatch(Connection conn, TableDescriptor table, String sqlStatement, List<JdbcSinkRecord> records) throws SQLException {
        try (PreparedStatement prepareStatement = conn.prepareStatement(sqlStatement)) {

            Stopwatch allbindStopwatch = Stopwatch.reusable();
            allbindStopwatch.start();

            // Bind column arrays for UNNEST using setArray()
            bindArraysForUnnest(table, records, conn, prepareStatement);

            allbindStopwatch.stop();
            LOGGER.trace("[PERF] All records bind execution time for UNNEST {}", allbindStopwatch.durations());

            Stopwatch executeStopwatch = Stopwatch.reusable();
            executeStopwatch.start();
            int updateCount = prepareStatement.executeUpdate();
            executeStopwatch.stop();

            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException("Execution failed for UNNEST batch", new int[]{ updateCount });
            }

            LOGGER.debug("UNNEST batch insert affected {} rows", updateCount);
            LOGGER.trace("[PERF] Execute UNNEST batch execution time {}", executeStopwatch.durations());
        }
    }

    /**
     * Bind arrays for UNNEST statement using PreparedStatement.setArray().
     * This approach ensures a single SQL query plan regardless of batch size.
     *
     * For INSERT/UPSERT: bind key fields first, then non-key fields
     * For UPDATE: bind non-key fields first, then key fields
     * For DELETE: bind only key fields
     */
    private void bindArraysForUnnest(TableDescriptor table, List<JdbcSinkRecord> records, Connection conn, PreparedStatement ps) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        JdbcSinkRecord firstRecord = records.get(0);
        int parameterIndex = 1;

        if (firstRecord.isDelete()) {
            bindKeyFieldArrays(table, records, conn, ps, parameterIndex);
        }
        else {
            switch (getConfig().getInsertMode()) {
                case INSERT:
                case UPSERT:
                    // For INSERT/UPSERT: key fields first, then non-key fields
                    parameterIndex = bindKeyFieldArrays(table, records, conn, ps, parameterIndex);
                    bindNonKeyFieldArrays(table, records, conn, ps, parameterIndex);
                    break;
                case UPDATE:
                    // For UPDATE: non-key fields first, then key fields
                    parameterIndex = bindNonKeyFieldArrays(table, records, conn, ps, parameterIndex);
                    bindKeyFieldArrays(table, records, conn, ps, parameterIndex);
                    break;
            }
        }
    }

    /**
     * Bind key field arrays using setArray().
     * Each column's values across all records are collected into an array and bound as a single parameter.
     */
    private int bindKeyFieldArrays(TableDescriptor table, List<JdbcSinkRecord> records, Connection conn, PreparedStatement ps, int startIndex) throws SQLException {
        JdbcSinkRecord firstRecord = records.get(0);
        Set<String> keyFieldNames = firstRecord.keyFieldNames();

        int parameterIndex = startIndex;
        for (String fieldName : keyFieldNames) {
            // Collect all values for this column
            List<Object> columnValues = new ArrayList<>(records.size());

            for (JdbcSinkRecord record : records) {
                final JdbcFieldDescriptor field = record.jdbcFields().get(fieldName);
                final Struct keySource = record.filteredKey();

                Object value = null;
                if (keySource != null) {
                    if (field.getSchema().isOptional()) {
                        value = keySource.getWithoutDefault(fieldName);
                    }
                    else {
                        value = keySource.get(fieldName);
                    }
                }

                columnValues.add(transformValue(table, record, field, value));
            }

            // Convert to array and bind using setArray()
            String sqlTypeName = getSqlTypeName(table, firstRecord, firstRecord.jdbcFields().get(fieldName));
            Array sqlArray = conn.createArrayOf(sqlTypeName, toElementArray(sqlTypeName, columnValues));
            ps.setArray(parameterIndex++, sqlArray);
        }

        return parameterIndex;
    }

    /**
     * Bind non-key field arrays using setArray().
     * Each column's values across all records are collected into an array and bound as a single parameter.
     */
    private int bindNonKeyFieldArrays(TableDescriptor table, List<JdbcSinkRecord> records, Connection conn, PreparedStatement ps, int startIndex) throws SQLException {
        JdbcSinkRecord firstRecord = records.get(0);
        Set<String> nonKeyFieldNames = firstRecord.nonKeyFieldNames();

        int parameterIndex = startIndex;
        for (String fieldName : nonKeyFieldNames) {
            // Collect all values for this column
            List<Object> columnValues = new ArrayList<>(records.size());

            for (JdbcSinkRecord record : records) {
                final JdbcFieldDescriptor field = record.jdbcFields().get(fieldName);
                final Struct payload = record.getPayload();

                Object value;
                if (field.getSchema().isOptional()) {
                    value = payload.getWithoutDefault(fieldName);
                }
                else {
                    value = payload.get(fieldName);
                }

                columnValues.add(transformValue(table, record, field, value));
            }

            // Convert to array and bind using setArray()
            String sqlTypeName = getSqlTypeName(table, firstRecord, firstRecord.jdbcFields().get(fieldName));
            Array sqlArray = conn.createArrayOf(sqlTypeName, toElementArray(sqlTypeName, columnValues));
            ps.setArray(parameterIndex++, sqlArray);
        }

        return parameterIndex;
    }

    /**
     * Converts the collected values to the array type required by {@link Connection#createArrayOf}.
     */
    private static Object[] toElementArray(String sqlTypeName, List<Object> columnValues) {
        // The PostgreSQL driver uses the Java component type to select its array encoder. A typed
        // byte[][] selects the bytea encoder; a byte[] element in Object[] is treated as ambiguous.
        if ("bytea".equals(sqlTypeName)) {
            return columnValues.toArray(new byte[0][]);
        }
        return columnValues.toArray();
    }

    private Object transformValue(TableDescriptor table, JdbcSinkRecord record, JdbcFieldDescriptor field, Object value) {
        List<ValueBindDescriptor> boundValues = maybeBindBytesAsCharacter(record, field, table, 1, value);
        if (boundValues == null) {
            boundValues = getDialect().bindValue(field, 1, value);
        }
        if (boundValues.size() != 1) {
            throw new ConnectException(
                    String.format("UNNEST does not support types that expand to multiple bind parameters (field: '%s', type: '%s'). "
                            + "Disable postgres.unnest.insert for this connector.", field.getName(),
                            getDialect().getSchemaType(field.getSchema()).getClass().getSimpleName()));
        }
        return boundValues.get(0).getValue();
    }

    /**
     * Get the SQL type name for createArrayOf() from the field descriptor.
     * Maps Kafka Connect schema types to PostgreSQL array element types.
     *
     * PostgreSQL createArrayOf() requires base type names without:
     * - Array brackets: text[] -> text
     * - Length modifiers: varchar(255) -> varchar
     * - Precision/scale: numeric(10,2) -> numeric
     */
    private String getSqlTypeName(TableDescriptor table, JdbcSinkRecord record, JdbcFieldDescriptor field) {
        // Encoded binary values use a text array to match the cast in the UNNEST statement.
        if (getDialect().resolveBinaryHandling(table, record, field).isEncoded()) {
            return "text";
        }

        final Schema schema = field.getSchema();
        final JdbcType type = getDialect().getSchemaType(schema);

        // The driver resolves the array element type by this name, so raw binary values must use
        // the canonical "bytea" name; dialect type names such as CockroachDB's "bytes" have no
        // corresponding server array type registered with the driver.
        if (BinaryHandling.isRawBytesSchema(schema, type)) {
            return "bytea";
        }

        String typeName = type.getTypeName(schema, field.isKey());

        // Remove array brackets: text[][] -> text
        typeName = typeName.replaceAll("\\[]", "").trim();

        // Remove length/precision modifiers: varchar(255) -> varchar, numeric(10,2) -> numeric
        int parenIndex = typeName.indexOf('(');
        if (parenIndex > 0) {
            typeName = typeName.substring(0, parenIndex).trim();
        }

        return typeName.toLowerCase();
    }

}

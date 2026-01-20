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
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.util.Stopwatch;

/**
 * UNNEST-optimized implementation for PostgreSQL that writes batches using SQL arrays.
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
                              JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
        super(session, queryBinderResolver, config, dialect);
    }

    @Override
    public void write(List<JdbcSinkRecord> records, SqlStatementInfo sqlStatementInfo) {
        // If it's a batch statement (UNNEST), use column-wise binding
        // Otherwise delegate to parent's standard row-wise binding
        if (sqlStatementInfo.isBatchStatement()) {
            writeUnnestBatch(records, sqlStatementInfo.statement());
        }
        else {
            super.write(records, sqlStatementInfo);
        }
    }

    /**
     * Write records using UNNEST approach with column-wise binding.
     */
    private void writeUnnestBatch(List<JdbcSinkRecord> records, String sqlStatement) {
        Stopwatch writeStopwatch = Stopwatch.reusable();
        writeStopwatch.start();
        final Transaction transaction = getSession().beginTransaction();

        try {
            getSession().doWork(processUnnestBatch(records, sqlStatement));
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
        writeStopwatch.stop();
        LOGGER.trace("[PERF] Total UNNEST write execution time {}", writeStopwatch.durations());
    }

    /**
     * Process a batch using UNNEST approach where each column's values are passed as a SQL array.
     * Uses PreparedStatement.setArray() for optimal performance and query plan caching.
     */
    private Work processUnnestBatch(List<JdbcSinkRecord> records, String sqlStatement) {
        return conn -> {
            try (PreparedStatement prepareStatement = conn.prepareStatement(sqlStatement)) {

                Stopwatch allbindStopwatch = Stopwatch.reusable();
                allbindStopwatch.start();

                // Bind column arrays for UNNEST using setArray()
                bindArraysForUnnest(records, conn, prepareStatement);

                allbindStopwatch.stop();
                LOGGER.trace("[PERF] All records bind execution time for UNNEST {}", allbindStopwatch.durations());

                Stopwatch executeStopwatch = Stopwatch.reusable();
                executeStopwatch.start();
                int updateCount = prepareStatement.executeUpdate();
                executeStopwatch.stop();

                // Check for execution failure
                if (updateCount == Statement.EXECUTE_FAILED) {
                    throw new BatchUpdateException("Execution failed for UNNEST batch", new int[]{ updateCount });
                }

                LOGGER.debug("UNNEST batch insert affected {} rows", updateCount);
                LOGGER.trace("[PERF] Execute UNNEST batch execution time {}", executeStopwatch.durations());
            }
        };
    }

    /**
     * Bind arrays for UNNEST statement using PreparedStatement.setArray().
     * This approach ensures a single SQL query plan regardless of batch size.
     *
     * For INSERT/UPSERT: bind key fields first, then non-key fields
     * For UPDATE: bind non-key fields first, then key fields
     * For DELETE: bind only key fields
     */
    private void bindArraysForUnnest(List<JdbcSinkRecord> records, Connection conn, PreparedStatement ps) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        JdbcSinkRecord firstRecord = records.get(0);
        int parameterIndex = 1;

        if (firstRecord.isDelete()) {
            parameterIndex = bindKeyFieldArrays(records, conn, ps, parameterIndex);
        }
        else {
            switch (getConfig().getInsertMode()) {
                case INSERT:
                case UPSERT:
                    // For INSERT/UPSERT: key fields first, then non-key fields
                    parameterIndex = bindKeyFieldArrays(records, conn, ps, parameterIndex);
                    parameterIndex = bindNonKeyFieldArrays(records, conn, ps, parameterIndex);
                    break;
                case UPDATE:
                    // For UPDATE: non-key fields first, then key fields
                    parameterIndex = bindNonKeyFieldArrays(records, conn, ps, parameterIndex);
                    parameterIndex = bindKeyFieldArrays(records, conn, ps, parameterIndex);
                    break;
            }
        }
    }

    /**
     * Bind key field arrays using setArray().
     * Each column's values across all records are collected into an array and bound as a single parameter.
     */
    private int bindKeyFieldArrays(List<JdbcSinkRecord> records, Connection conn, PreparedStatement ps, int startIndex) throws SQLException {
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

                columnValues.add(value);
            }

            // Convert to array and bind using setArray()
            String sqlTypeName = getSqlTypeName(firstRecord.jdbcFields().get(fieldName));
            Array sqlArray = conn.createArrayOf(sqlTypeName, columnValues.toArray());
            ps.setArray(parameterIndex++, sqlArray);
        }

        return parameterIndex;
    }

    /**
     * Bind non-key field arrays using setArray().
     * Each column's values across all records are collected into an array and bound as a single parameter.
     */
    private int bindNonKeyFieldArrays(List<JdbcSinkRecord> records, Connection conn, PreparedStatement ps, int startIndex) throws SQLException {
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

                columnValues.add(value);
            }

            // Convert to array and bind using setArray()
            String sqlTypeName = getSqlTypeName(firstRecord.jdbcFields().get(fieldName));
            Array sqlArray = conn.createArrayOf(sqlTypeName, columnValues.toArray());
            ps.setArray(parameterIndex++, sqlArray);
        }

        return parameterIndex;
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
    private String getSqlTypeName(JdbcFieldDescriptor field) {
        Schema schema = field.getSchema();
        JdbcType type = getDialect().getSchemaType(schema);
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

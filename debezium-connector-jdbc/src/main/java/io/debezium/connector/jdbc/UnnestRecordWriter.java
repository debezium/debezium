/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.util.Stopwatch;

/**
 * UNNEST-optimized implementation for PostgreSQL that writes batches using column-wise binding.
 * This approach can provide 5-10x performance improvement for bulk inserts/upserts.
 *
 * For batch statements (isBatchStatement=true), uses UNNEST with column-wise binding.
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
     * Process a batch using UNNEST approach where values are bound column-wise rather than row-wise.
     * For UNNEST, we bind all values for column 1 across all records, then all values for column 2, etc.
     */
    private Work processUnnestBatch(List<JdbcSinkRecord> records, String sqlStatement) {
        return conn -> {
            try (PreparedStatement prepareStatement = conn.prepareStatement(sqlStatement)) {

                QueryBinder queryBinder = getQueryBinderResolver().resolve(prepareStatement);
                Stopwatch allbindStopwatch = Stopwatch.reusable();
                allbindStopwatch.start();

                // Bind values column-wise for UNNEST
                bindValuesForUnnest(records, queryBinder);

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
     * Bind values for UNNEST statement where each column's values from all records are bound together.
     * For INSERT/UPSERT: bind key fields first, then non-key fields
     * For UPDATE: bind non-key fields first, then key fields
     * For DELETE: bind only key fields
     */
    private void bindValuesForUnnest(List<JdbcSinkRecord> records, QueryBinder queryBinder) {
        if (records.isEmpty()) {
            return;
        }

        JdbcSinkRecord firstRecord = records.get(0);
        int parameterIndex = 1;

        if (firstRecord.isDelete()) {
            parameterIndex = bindKeyValuesColumnWise(records, queryBinder, parameterIndex);
        }
        else {
            switch (getConfig().getInsertMode()) {
                case INSERT:
                case UPSERT:
                    // For INSERT/UPSERT: key fields first, then non-key fields
                    parameterIndex = bindKeyValuesColumnWise(records, queryBinder, parameterIndex);
                    parameterIndex = bindNonKeyValuesColumnWise(records, queryBinder, parameterIndex);
                    break;
                case UPDATE:
                    // For UPDATE: non-key fields first, then key fields
                    parameterIndex = bindNonKeyValuesColumnWise(records, queryBinder, parameterIndex);
                    parameterIndex = bindKeyValuesColumnWise(records, queryBinder, parameterIndex);
                    break;
            }
        }
    }

    /**
     * Bind key field values column-wise across all records.
     */
    private int bindKeyValuesColumnWise(List<JdbcSinkRecord> records, QueryBinder queryBinder, int startIndex) {
        JdbcSinkRecord firstRecord = records.get(0);
        Set<String> keyFieldNames = firstRecord.keyFieldNames();

        int parameterIndex = startIndex;
        for (String fieldName : keyFieldNames) {
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

                List<ValueBindDescriptor> boundValues = getDialect().bindValue(field, parameterIndex, value);
                boundValues.forEach(queryBinder::bind);
                parameterIndex += boundValues.size();
            }
        }

        return parameterIndex;
    }

    /**
     * Bind non-key field values column-wise across all records.
     */
    private int bindNonKeyValuesColumnWise(List<JdbcSinkRecord> records, QueryBinder queryBinder, int startIndex) {
        JdbcSinkRecord firstRecord = records.get(0);
        Set<String> nonKeyFieldNames = firstRecord.nonKeyFieldNames();

        int parameterIndex = startIndex;
        for (String fieldName : nonKeyFieldNames) {
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

                List<ValueBindDescriptor> boundValues = getDialect().bindValue(field, parameterIndex, value);
                boundValues.forEach(queryBinder::bind);
                parameterIndex += boundValues.size();
            }
        }

        return parameterIndex;
    }
}

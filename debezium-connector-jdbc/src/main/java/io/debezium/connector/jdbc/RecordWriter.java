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
import java.util.Objects;

import org.apache.kafka.connect.data.Struct;
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
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

    public void write(List<JdbcSinkRecord> records, String sqlStatement) {
        Stopwatch writeStopwatch = Stopwatch.reusable();
        writeStopwatch.start();
        final Transaction transaction = session.beginTransaction();

        try {
            session.doWork(processBatch(records, sqlStatement));
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
        writeStopwatch.stop();
        LOGGER.trace("[PERF] Total write execution time {}", writeStopwatch.durations());
    }

    private Work processBatch(List<JdbcSinkRecord> records, String sqlStatement) {
        return conn -> {
            try (PreparedStatement prepareStatement = conn.prepareStatement(sqlStatement)) {

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
        if (Objects.requireNonNull(config.getPrimaryKeyMode()) == JdbcSinkConnectorConfig.PrimaryKeyMode.KAFKA) {
            query.bind(new ValueBindDescriptor(index++, record.topicName()));
            query.bind(new ValueBindDescriptor(index++, record.partition()));
            query.bind(new ValueBindDescriptor(index++, record.offset()));
        }
        else {
            final Struct keySource = record.getKeyStruct(config.getPrimaryKeyMode(), config.getPrimaryKeyFields());
            if (keySource != null) {
                index = bindFieldValuesToQuery(record, query, index, keySource, record.keyFieldNames());
            }
        }
        return index;
    }

    private int bindNonKeyValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index) {
        return bindFieldValuesToQuery(record, query, index, record.getPayload(), record.getNonKeyFieldNames());
    }

    private int bindFieldValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index, Struct source, List<String> fields) {
        for (String fieldName : fields) {
            final JdbcSinkRecord.FieldDescriptor field = record.allFields().get(fieldName);

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

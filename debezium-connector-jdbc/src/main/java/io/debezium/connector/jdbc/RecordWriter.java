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

import io.debezium.connector.jdbc.dialect.DatabaseDialect;

/**
 * Effectively writes the batches using Hibernate {@link Work}
 *
 * @author Mario Fiore Vitale
 */
public class RecordWriter {

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

    public void write(List<SinkRecordDescriptor> records, String sqlStatement) {

        final Transaction transaction = session.beginTransaction();

        try {
            session.doWork(processBatch(records, sqlStatement));
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
    }

    private Work processBatch(List<SinkRecordDescriptor> records, String sqlStatement) {

        return conn -> {

            try (PreparedStatement prepareStatement = conn.prepareStatement(sqlStatement)) {

                QueryBinder queryBinder = queryBinderResolver.resolve(prepareStatement);
                for (SinkRecordDescriptor sinkRecordDescriptor : records) {

                    bindValues(sinkRecordDescriptor, queryBinder);

                    prepareStatement.addBatch();
                }

                int[] batchResult = prepareStatement.executeBatch();
                for (int updateCount : batchResult) {
                    if (updateCount == Statement.EXECUTE_FAILED) {
                        throw new BatchUpdateException("Execution failed for part of the batch", batchResult);
                    }
                }
            }
        };
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
            final SinkRecordDescriptor.FieldDescriptor field = record.getFields().get(fieldName);

            Object value;
            if (field.getSchema().isOptional()) {
                value = source.getWithoutDefault(fieldName);
            } else {
                value = source.get(fieldName);
            }
            List<ValueBindDescriptor> boundValues = dialect.bindValue(field, index, value);

            boundValues.forEach(query::bind);
            index += boundValues.size();
        }
        return index;
    }
}

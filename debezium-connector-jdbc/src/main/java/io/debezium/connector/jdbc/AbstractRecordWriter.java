/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.hibernate.SharedSessionContract;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Abstract base class for RecordWriter implementations.
 * Provides common functionality for binding values to queries.
 *
 * @author Gaurav Miglani
 */
public abstract class AbstractRecordWriter implements RecordWriter {

    private final SharedSessionContract session;
    private final QueryBinderResolver queryBinderResolver;
    private final JdbcSinkConnectorConfig config;
    private final DatabaseDialect dialect;

    protected AbstractRecordWriter(SharedSessionContract session, QueryBinderResolver queryBinderResolver,
                                   JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
        this.session = session;
        this.queryBinderResolver = queryBinderResolver;
        this.config = config;
        this.dialect = dialect;
    }

    protected SharedSessionContract getSession() {
        return session;
    }

    protected QueryBinderResolver getQueryBinderResolver() {
        return queryBinderResolver;
    }

    protected JdbcSinkConnectorConfig getConfig() {
        return config;
    }

    protected DatabaseDialect getDialect() {
        return dialect;
    }

    /**
     * Bind key field values to the query for a single record.
     */
    protected int bindKeyValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index) {
        final Struct keySource = record.filteredKey();
        if (keySource != null) {
            index = bindFieldValuesToQuery(record, query, index, keySource, record.keyFieldNames());
        }
        return index;
    }

    /**
     * Bind non-key field values to the query for a single record.
     */
    protected int bindNonKeyValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index) {
        return bindFieldValuesToQuery(record, query, index, record.getPayload(), record.nonKeyFieldNames());
    }

    /**
     * Bind field values to the query for a single record.
     */
    protected int bindFieldValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index, Struct source, Set<String> fieldNames) {
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

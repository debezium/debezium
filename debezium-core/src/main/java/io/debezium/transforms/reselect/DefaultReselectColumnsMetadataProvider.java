/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.reselect;

import java.util.List;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.relational.TableId;

/**
 * Default re-select columns metadata provider implementation.
 *
 * This implementation simply fetches the current re-select column's state from the current row, if exists,
 * and performs no conversion of the re-selected column.
 *
 * @author Chris Cranford
 */
public class DefaultReselectColumnsMetadataProvider<R extends ConnectRecord<R>> implements ReselectColumnsMetadataProvider<R> {

    @Override
    public String getName() {
        return "default";
    }

    @Override
    public String getQuery(R record, Struct source, List<String> columns, TableId tableId) {
        final String tableName = String.format("%s.%s", tableId.schema(), tableId.table());
        final StringBuilder query = new StringBuilder();
        query.append("SELECT ").append(String.join(",", columns));
        query.append(" FROM ").append(tableName);
        query.append(" WHERE ").append(createPrimaryKeyWhereClause(record));
        return query.toString();
    }

    @Override
    public Object convertValue(int jdbcType, Object value) {
        // todo: we need to consider how to inject ValueConverters into this :(
        return value;
    }

    protected String createPrimaryKeyWhereClause(R record) {
        Object keyValue = record.key();
        if (!(keyValue instanceof Struct)) {
            throw new ConnectException("Cannot re-reselect columns with a key-less table");
        }
        final Struct key = (Struct) record.key();
        final List<org.apache.kafka.connect.data.Field> fields = record.keySchema().fields();
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            org.apache.kafka.connect.data.Field field = fields.get(i);
            sb.append(field.name()).append("=").append(getKeyFieldValue(key, field));
            if ((i + 1) < fields.size()) {
                sb.append(" AND ");
            }
        }
        return sb.toString();
    }

    protected Object getKeyFieldValue(Struct key, org.apache.kafka.connect.data.Field field) {
        switch (field.schema().type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                return key.get(field);
            case BOOLEAN:
                return key.getBoolean(field.name()) ? 1 : 0;
            default:
                return String.format("'%s'", (String) key.get(field));
        }
    }

}

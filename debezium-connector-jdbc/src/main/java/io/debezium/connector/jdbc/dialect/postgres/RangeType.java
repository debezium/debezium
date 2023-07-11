/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.query.Query;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.ColumnDescriptor;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.Type;

/**
 * An implementation of {@link Type} for {@code INT4RANGE}, {@code INT8RANGE}, {@code NUMRANGE},
 * {@code TSRANGE}, {@code TZSTZRANGE}, and {@code DATERANGE} column types.
 *
 * @author Chris Cranford
 */
class RangeType extends AbstractType {

    public static final RangeType INSTANCE = new RangeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "INT4RANGE", "INT8RANGE", "NUMRANGE", "TSRANGE", "TSTZRANGE", "DATERANGE" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "cast(? as " + getSourceColumnType(schema).orElseThrow() + ")";
    }

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        return getSourceColumnType(schema).orElseThrow();
    }

    @Override
    public int bind(Query<?> query, int index, Schema schema, Object value) {
        query.setParameter(index, value == null ? null : ((String) value).replaceAll("\"", ""));

        return 1;
    }
}

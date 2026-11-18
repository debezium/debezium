/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.sql.Types;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;

/**
 * An abstract temporal implementation of {@link JdbcType} for {@code TIME} based columns.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTimeType extends AbstractTemporalType {

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // NOTE:
        // The MySQL connector does not use the __debezium.source.column.scale parameter to pass
        // the time column's precision but instead uses the __debezium.source.column.length key
        // which differs from all other connector implementations.
        //
        final int precision = getSchemaTimePrecision(schema);
        final DatabaseDialect dialect = getDialect();
        return dialect.getJdbcTypeName(Types.TIME, Size.precision(precision));
    }

    protected int getSchemaTimePrecision(Schema schema) {
        final int precision = getTimePrecision(schema);
        final DatabaseDialect dialect = getDialect();
        // Use the timestamp precision bound for source TIME types because Oracle maps them to
        // timestamp-based target columns.
        if (shouldUseSourcePrecision(precision, dialect.getDefaultTimestampPrecision())) {
            return precision;
        }
        return dialect.getDefaultTimePrecision();
    }

    protected int getTimePrecision(Schema schema) {
        final String length = getSourceColumnSize(schema).orElse("0");
        final Optional<String> scale = getSourceColumnPrecision(schema);
        return scale.map(Integer::parseInt).orElseGet(() -> Integer.parseInt(length));
    }

    protected boolean shouldUseSourcePrecision(int precision, int maxPrecision) {
        return precision > 0 && precision <= maxPrecision;
    }
}

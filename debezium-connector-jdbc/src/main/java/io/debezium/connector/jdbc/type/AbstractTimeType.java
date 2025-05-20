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
 * An abstract temporal implementation of {@link Type} for {@code TIME} based columns.
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
        final int precision = getTimePrecision(schema);
        DatabaseDialect dialect = getDialect();
        // We use TIMESTAMP here even for source TIME types as Oracle will use DATE types for
        // such columns, and it only supports second-based precision.
        if (precision > 0 && precision <= dialect.getDefaultTimestampPrecision()) {
            return dialect.getJdbcTypeName(Types.TIME, Size.precision(precision));
        }
        return dialect.getJdbcTypeName(Types.TIME);
    }

    protected int getTimePrecision(Schema schema) {
        final String length = getSourceColumnSize(schema).orElse("0");
        final Optional<String> scale = getSourceColumnPrecision(schema);
        return scale.map(Integer::parseInt).orElseGet(() -> Integer.parseInt(length));
    }
}

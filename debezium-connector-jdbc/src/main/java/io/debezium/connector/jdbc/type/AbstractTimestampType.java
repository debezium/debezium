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
 * An abstract temporal implementation of {@link Type} for {@code TIMESTAMP} based columns.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTimestampType extends AbstractTemporalType {

    @Override
    public String getTypeName(DatabaseDialect dialect, Schema schema, boolean key) {
        final int precision = getTimePrecision(schema);
        if (precision > 0 && precision <= dialect.getMaxTimestampPrecision()) {
            return dialect.getTypeName(getJdbcType(), Size.precision(precision));
        }
        return dialect.getTypeName(getJdbcType());
    }

    protected int getTimePrecision(Schema schema) {
        final String length = getSourceColumnSize(schema).orElse("0");
        final Optional<String> scale = getSourceColumnPrecision(schema);
        return scale.map(Integer::parseInt).orElseGet(() -> Integer.parseInt(length));
    }

    protected int getJdbcType() {
        return Types.TIMESTAMP;
    }

}

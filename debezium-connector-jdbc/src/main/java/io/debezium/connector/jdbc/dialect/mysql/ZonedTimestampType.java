/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.time.ZonedDateTime;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.type.DefaultValueContext;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.type.debezium.DebeziumZonedTimestampType;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.SchemaUtils;

/**
 * An implementation of {@link JdbcType} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimestampType extends DebeziumZonedTimestampType {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZonedTimestampType.class);

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();

    @Override
    protected int getJdbcBindType() {
        return Types.TIMESTAMP; // TIMESTAMP_WITH_TIMEZONE not supported
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return getDefaultValueBinding(schema, value,
                new DefaultValueContext(null, SchemaUtils.getSourceColumnName(schema).orElse(null)));
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value, DefaultValueContext context) {
        if (value == null) {
            return super.getDefaultValueBinding(schema, value);
        }

        final ZonedDateTime parsed = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER);
        final ZonedDateTime clamped = clampToDialectBounds(parsed);
        final String binding = getDialect().getFormattedTimestampWithTimeZone(ZonedTimestamp.toIsoString(clamped, null, null));
        if (!clamped.isEqual(parsed)) {
            LOGGER.warn("Default value '{}' of column '{}' in table '{}' is out of range for the target database and was clamped to {}.",
                    value,
                    context == null || context.columnName() == null ? "unknown" : context.columnName(),
                    context == null || context.collectionId() == null ? "unknown" : context.collectionId().toFullIdentiferString(),
                    binding);
        }
        return binding;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import java.sql.Types;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.type.debezium.DebeziumZonedTimestampType;
import io.debezium.time.ZonedTimestamp;

import static com.fasterxml.jackson.databind.type.LogicalType.DateTime;
import static io.debezium.connector.jdbc.dialect.oracle.ZonedTimestampType.MINIMUM_TIMESTAMP;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
public class ZonedTimestampType extends DebeziumZonedTimestampType {

    public static final ZonedTimestampType INSTANCE = new ZonedTimestampType();
//    TODO: Should the new config value be carried down through call stack or be directly accessible in the file
//    private final JdbcSinkConnectorConfig config = new JdbcSinkConnectorConfig();
    public static final String MINIMUM_TIMESTAMP = "1970-01-01T00:00:01+00:00";
    public static final String MAXIMUM_TIMESTAMP = "2038-01-19T03:14:07+00:00";

    @Override
    protected int getJdbcBindType() {
        return Types.TIMESTAMP; // TIMESTAMP_WITH_TIMEZONE not supported
    }

    @Override
    protected Object convertInfinityTimestampValue(Object value) {
//        if (config. ORACLE_INFINITY_CONVERTER_ENABLE) {
//            return value;
//        }

        final ZonedDateTime zdt;
        zdt = ZonedDateTime.parse((String) value, ZonedTimestamp.FORMATTER).withZoneSameInstant(ZoneId.ofOffset("UTC", ZoneOffset.UTC));

        if (ZonedDateTime.parse(io.debezium.connector.jdbc.dialect.oracle.ZonedTimestampType.MINIMUM_TIMESTAMP).equals(zdt)) {
            return ZonedDateTime.parse(MINIMUM_TIMESTAMP, ZonedTimestamp.FORMATTER).withZoneSameInstant(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
        } else if (ZonedDateTime.parse(io.debezium.connector.jdbc.dialect.oracle.ZonedTimestampType.MINIMUM_TIMESTAMP).equals(zdt)) {
            return ZonedDateTime.parse(MAXIMUM_TIMESTAMP, ZonedTimestamp.FORMATTER).withZoneSameInstant(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
        }
        return value;
    }
}

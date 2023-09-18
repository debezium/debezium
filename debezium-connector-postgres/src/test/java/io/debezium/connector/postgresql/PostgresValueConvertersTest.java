/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.ZoneOffset;

import org.junit.Test;
import org.postgresql.PGStatement;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;

public class PostgresValueConvertersTest {
    @Test
    public void testInfinityTimestampVales() {

        PostgresValueConverter postgresValueConverter = new PostgresValueConverter(Charset.defaultCharset(),
                JdbcValueConverters.DecimalMode.DOUBLE,
                TemporalPrecisionMode.CONNECT, ZoneOffset.UTC,
                JdbcValueConverters.BigIntUnsignedMode.LONG,
                false,
                null,
                PostgresConnectorConfig.HStoreHandlingMode.JSON,
                CommonConnectorConfig.BinaryHandlingMode.BYTES,
                PostgresConnectorConfig.IntervalHandlingMode.NUMERIC,
                null,
                1);

        Object positiveData = new Timestamp(PGStatement.DATE_POSITIVE_INFINITY);
        assertThat(postgresValueConverter.convertTimestampToLocalDateTime(null, null, positiveData)
                .equals(PostgresValueConverter.POSITIVE_INFINITY));

        Object negativeData = new Timestamp(PGStatement.DATE_NEGATIVE_INFINITY);
        assertThat(postgresValueConverter.convertTimestampToLocalDateTime(null, null, negativeData)
                .equals(PostgresValueConverter.NEGATIVE_INFINITY));
    }
}

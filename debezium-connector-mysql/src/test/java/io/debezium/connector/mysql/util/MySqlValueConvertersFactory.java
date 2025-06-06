/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.util;

import java.time.temporal.TemporalAdjuster;

import io.confluent.credentialproviders.DefaultJdbcCredentialsProvider;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogValueConvertersFactory;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;

/**
 * Implementation of {@link BinlogValueConvertersFactory} for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlValueConvertersFactory implements BinlogValueConvertersFactory<MySqlValueConverters> {
    @Override
    public MySqlValueConverters create(Configuration configuration, TemporalAdjuster temporalAdjuster) {
        final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(configuration, new DefaultJdbcCredentialsProvider());
        return new MySqlValueConverters(
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(),
                connectorConfig.binaryHandlingMode(),
                temporalAdjuster,
                connectorConfig.getEventConvertingFailureHandlingMode(),
                connectorConfig.getServiceRegistry());
    }
}

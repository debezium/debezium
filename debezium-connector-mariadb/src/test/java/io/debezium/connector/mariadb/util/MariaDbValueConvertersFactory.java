/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.util;

import java.time.temporal.TemporalAdjuster;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.util.BinlogValueConvertersFactory;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.connector.mariadb.jdbc.MariaDbValueConverters;

/**
 * Implementation of {@link BinlogValueConvertersFactory} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbValueConvertersFactory implements BinlogValueConvertersFactory<MariaDbValueConverters> {
    @Override
    public MariaDbValueConverters create(Configuration configuration, TemporalAdjuster temporalAdjuster) {
        final MariaDbConnectorConfig connectorConfig = new MariaDbConnectorConfig(configuration);
        return new MariaDbValueConverters(
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(),
                connectorConfig.binaryHandlingMode(),
                temporalAdjuster,
                connectorConfig.getEventConvertingFailureHandlingMode(),
                connectorConfig.getServiceRegistry());
    }
}

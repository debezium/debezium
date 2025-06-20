/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0,
 * available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.BaseDefaultValueConverter;
import io.debezium.relational.ValueConverterProvider;

/**
 * CockroachDB-specific implementation of {@link JdbcValueConverters},
 * applying Debezium's standard mappings with sensible defaults for CockroachDB.
 *
 * @author Virag Tripathi
 */
public class CockroachDBValueConverter extends JdbcValueConverters {

    public CockroachDBValueConverter(CommonConnectorConfig connectorConfig) {
        super(
                JdbcValueConverters.DecimalMode.PRECISE,
                connectorConfig.binaryHandlingMode(),
                connectorConfig.timePrecisionMode(),
                connectorConfig.booleanLiteralConverterMode(),
                connectorConfig.bigIntUnsignedHandlingMode());
    }

    public static ValueConverterProvider valueConverterProvider(CommonConnectorConfig connectorConfig) {
        return new BaseDefaultValueConverter(new CockroachDBValueConverter(connectorConfig));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Field;

/**
 * Configuration fields for {@link NumberToZeroScaleConverter}.
 */
public class NumberToZeroScaleConverterConfig {

    public static final Field DECIMAL_MODE = Field.create("decimal.mode")
            .withDisplayName("Decimal handling mode")
            .withType(ConfigDef.Type.STRING)
            .withDefault("PRECISE")
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Specifies how DECIMAL and NUMERIC columns should be represented in change events. " +
                    "Options are: PRECISE (default) - uses java.math.BigDecimal, DOUBLE - uses double, STRING - encodes values as formatted strings.");
}

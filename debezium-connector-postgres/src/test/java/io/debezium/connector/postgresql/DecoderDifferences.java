/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

/**
 * A class that contains assertions or expected values tailored to the behaviour of a concrete decoder plugin
 *
 * @author Jiri Pechanec
 *
 */
public class DecoderDifferences {
    static final String TOASTED_VALUE_PLACEHOLDER = "__debezium_unavailable_value";

    private static boolean pgoutput() {
        return TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT;
    }

    public static String optionalToastedValuePlaceholder() {
        return TOASTED_VALUE_PLACEHOLDER;
    }

    public static String mandatoryToastedValuePlaceholder() {
        return TOASTED_VALUE_PLACEHOLDER;
    }

    public static boolean areDefaultValuesRefreshedEagerly() {
        return pgoutput();
    }
}

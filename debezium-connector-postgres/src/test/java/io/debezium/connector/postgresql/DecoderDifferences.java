/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class that contains assertions or expected values tailored to the behaviour of a concrete decoder plugin
 *
 * @author Jiri Pechanec
 *
 */
public class DecoderDifferences {
    static final String TOASTED_VALUE_PLACEHOLDER = "__debezium_unavailable_value";
    static final String TOASTED_VALUE_NUMBER_STRING = "95, 95, 100, 101, 98, 101, 122, 105, 117, 109, 95, 117, 110, 97, 118, 97, 105, 108, 97, 98, 108, 101, 95, 118, 97, 108, 117, 101";

    private static boolean pgoutput() {
        return TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT;
    }

    public static String optionalToastedValuePlaceholder() {
        return TOASTED_VALUE_PLACEHOLDER;
    }

    public static String mandatoryToastedValuePlaceholder() {
        return TOASTED_VALUE_PLACEHOLDER;
    }

    public static byte[] mandatoryToastedValueBinaryPlaceholder() {
        return PostgresConnectorConfig.UNAVAILABLE_VALUE_PLACEHOLDER.defaultValueAsString().getBytes();
    }

    public static List<Integer> toastedValueIntPlaceholder() {
        return Stream.of(TOASTED_VALUE_NUMBER_STRING.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
    }

    public static List<Long> toastedValueBigintPlaceholder() {
        return Stream.of(TOASTED_VALUE_NUMBER_STRING.split(","))
                .map(String::trim)
                .map(Long::parseLong)
                .collect(Collectors.toList());
    }

    public static boolean areDefaultValuesRefreshedEagerly() {
        return pgoutput();
    }
}

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
    static final String TOASTED_VALUE_NUMBER_STRING = "28694827,62948267,94852637,62847362,94837264,73486259,29483726,43827619,84692735,29873462";

    private static boolean pgoutput() {
        return TestHelper.decoderPlugin() == PostgresConnectorConfig.LogicalDecoder.PGOUTPUT;
    }

    public static String optionalToastedValuePlaceholder() {
        return TOASTED_VALUE_PLACEHOLDER;
    }

    public static String mandatoryToastedValuePlaceholder() {
        return TOASTED_VALUE_PLACEHOLDER;
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

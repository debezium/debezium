/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.regex.Pattern;

/**
 * Utility class providing shared validation helpers for connector configuration.
 */
public final class ConnectorConfigValidationHelper {

    /**
     * The prefix marking a configuration value whose remainder is a string of hex encoded octets.
     */
    public static final String HEX_PREFIX = "hex:";

    private static final Pattern HEX_OCTETS_PATTERN = Pattern.compile("([0-9a-fA-F]{2})*");

    private ConnectorConfigValidationHelper() {
    }

    /**
     * Validates that an include list and an exclude list are not both specified for the same filter type.
     * When both are present, a validation problem is reported on the exclude field.
     *
     * @param config the configuration
     * @param includeField the include list field
     * @param excludeField the exclude list field
     * @param problems the validation output
     * @return 1 if both lists are specified, 0 otherwise
     */
    public static int validateExcludeField(Configuration config,
                                           Field includeField,
                                           Field excludeField,
                                           Field.ValidationOutput problems) {
        final String includeList = config.getString(includeField);
        final String excludeList = config.getString(excludeField);

        if (includeList != null && excludeList != null) {
            problems.accept(excludeField, excludeList, "\"%s\" is already specified".formatted(includeField.name()));
            return 1;
        }
        return 0;
    }

    /**
     * Validates that a value starting with the {@link #HEX_PREFIX} prefix is followed by hex encoded octets,
     * i.e. an even number of hexadecimal characters. Values without the prefix are not validated.
     *
     * @param config the configuration
     * @param field the field to validate
     * @param problems the validation output
     * @return 1 if the value has the prefix but is not valid hex, 0 otherwise
     */
    public static int validateHexPrefixedValue(Configuration config, Field field, Field.ValidationOutput problems) {
        final String value = config.getString(field);
        if (value != null && value.startsWith(HEX_PREFIX)
                && !HEX_OCTETS_PATTERN.matcher(value.substring(HEX_PREFIX.length())).matches()) {
            problems.accept(field, value, "The value after the '%s' prefix must consist of an even number of hexadecimal characters".formatted(HEX_PREFIX));
            return 1;
        }
        return 0;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

/**
 * Utility class providing shared validation helpers for connector configuration.
 */
public final class ConnectorConfigValidationHelper {

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
}

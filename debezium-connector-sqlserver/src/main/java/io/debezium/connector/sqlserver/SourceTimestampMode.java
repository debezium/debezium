/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.util.Arrays;

import io.debezium.config.EnumeratedValue;

/**
 * Strategy for populating the source.ts_ms field in change events.
 */
public enum SourceTimestampMode implements EnumeratedValue {

    /**
     * This mode (default) will set the source timestamp field (ts_ms) of when the record was committed in the database.
     */
    COMMIT("commit"),

    /**
     * This mode will set the source timestamp field (ts_ms) of when the record was processed by Debezium.
     */
    PROCESSING("processing");

    private final String value;

    SourceTimestampMode(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public static SourceTimestampMode getDefaultMode() {
        return COMMIT;
    }

    static SourceTimestampMode fromMode(String mode) {
        return Arrays.stream(SourceTimestampMode.values())
                .filter(s -> s.name().equalsIgnoreCase(mode))
                .findFirst()
                .orElseGet(SourceTimestampMode::getDefaultMode);
    }
}

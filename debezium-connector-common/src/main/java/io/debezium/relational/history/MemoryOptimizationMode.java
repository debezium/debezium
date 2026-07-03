/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import io.debezium.config.EnumeratedValue;
import io.debezium.util.Interner;

/**
 * Controls how Debezium deduplicates identical schema objects in memory using an interner.
 *
 * @see SchemaHistory#MEMORY_OPTIMIZATION
 */
public enum MemoryOptimizationMode implements EnumeratedValue {

    /** No deduplication is performed. */
    OFF("off"),

    /** Each connector uses its own isolated intern pool. */
    ON("on"),

    /** All connectors share a single JVM-wide pool, maximising deduplication across similar schemas. */
    SHARED("shared");

    private final String value;

    MemoryOptimizationMode(String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    public Interner.Mode toInternerMode() {
        return switch (this) {
            case OFF -> Interner.Mode.OFF;
            case ON -> Interner.Mode.ON;
            case SHARED -> Interner.Mode.SHARED;
        };
    }

    public static MemoryOptimizationMode parse(String value) {
        if (value == null || value.isBlank()) {
            return OFF;
        }
        MemoryOptimizationMode result = EnumeratedValue.parse(MemoryOptimizationMode.class, value);
        if (result == null) {
            throw new IllegalArgumentException(
                    "Invalid memory optimization mode: '" + value + "'. Valid values are: off, on, shared");
        }
        return result;
    }
}

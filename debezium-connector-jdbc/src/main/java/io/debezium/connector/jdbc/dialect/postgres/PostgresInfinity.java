/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import java.sql.Types;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;

import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredTemporal;

/**
 * Binds infinite structured dates and timestamps as the {@code infinity} and {@code -infinity} values of PostgreSQL.
 */
final class PostgresInfinity {

    private static final String POSITIVE_INFINITY = "infinity";
    private static final String NEGATIVE_INFINITY = "-infinity";

    /**
     * Returns the PostgreSQL value of an infinite structured value.
     *
     * @param supported whether the target type supports infinity
     * @param value the value; may be null
     * @return {@code infinity} or {@code -infinity}, or empty if the target does not support infinity or the value is finite
     */
    static Optional<String> value(boolean supported, Object value) {
        if (supported && value instanceof Struct struct) {
            if (StructuredTemporal.isPositiveInfinity(struct)) {
                return Optional.of(POSITIVE_INFINITY);
            }
            if (StructuredTemporal.isNegativeInfinity(struct)) {
                return Optional.of(NEGATIVE_INFINITY);
            }
        }
        return Optional.empty();
    }

    static Optional<String> defaultValueBinding(boolean supported, Object value) {
        return value(supported, value).map(infinity -> "'" + infinity + "'");
    }

    static Optional<List<ValueBindDescriptor>> bind(boolean supported, int index, Object value) {
        return value(supported, value).map(infinity -> List.of(new ValueBindDescriptor(index, infinity, Types.VARCHAR)));
    }

    /**
     * Returns whether the value is a special value that the target stores as is, so that it needs no range or precision
     * validation.
     */
    static boolean isStoredAsIs(boolean supported, Object value) {
        return supported && value instanceof Struct struct && !StructuredTemporal.isFinite(struct);
    }

    private PostgresInfinity() {
    }
}

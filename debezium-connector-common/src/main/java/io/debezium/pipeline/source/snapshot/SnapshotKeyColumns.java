/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.math.BigDecimal;
import java.util.Optional;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.ValueWrapper;

/**
 * Helpers for working with snapshot key-column values, shared by the incremental snapshot and by the initial
 * snapshot's resume-from-key handling so that neither has to reach into the other.
 */
public final class SnapshotKeyColumns {

    private SnapshotKeyColumns() {
    }

    /**
     * Unwraps a raw row value so that it can be bound back into a chunk or resume query parameter.
     */
    @SuppressWarnings("unchecked")
    public static Object unwrapKeyValue(Object fieldValue) {
        if (fieldValue instanceof SpecialValueDecimal specialValueDecimal) {
            final Optional<BigDecimal> decimalValue = specialValueDecimal.getDecimalValue();
            if (decimalValue.isPresent()) {
                return decimalValue.get();
            }
            // NaN and the infinities have no BigDecimal form: getWrappedValue() would degrade them to null,
            // which the chunk query cannot bind. Their double form stays bindable and PostgreSQL orders it
            // consistently with the numeric column (NaN above everything).
            return specialValueDecimal.toDouble();
        }
        return fieldValue instanceof ValueWrapper<?> ? ((ValueWrapper<Object>) fieldValue).getWrappedValue() : fieldValue;
    }
}

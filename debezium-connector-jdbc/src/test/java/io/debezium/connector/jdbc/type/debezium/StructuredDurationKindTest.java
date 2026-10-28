/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type.debezium;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.time.StructuredDuration;

@Tag("UnitTests")
class StructuredDurationKindTest {

    @Test
    @DisplayName("Should prefer the declared duration kind over the components of the value")
    void shouldPreferDeclaredDurationKind() {
        final var schema = StructuredDuration.builder(6, StructuredDuration.Kind.MONTH_DAY_TIME).build();
        final var value = StructuredDuration.from(schema, 0, 0, 0, 1, 2, 3, 0, 6);

        assertThat(StructuredTemporalPreflightValidator.durationKind(schema, value)).isEqualTo(StructuredDuration.Kind.MONTH_DAY_TIME);
    }

    @Test
    @DisplayName("Should infer the duration kind from the value only if its components identify the kind")
    void shouldInferDurationKindWithoutDeclaredKind() {
        // A schema from an earlier version declares no kind
        final var schema = StructuredDuration.schema();

        assertThat(StructuredTemporalPreflightValidator.durationKind(schema, null)).isEqualTo(StructuredDuration.Kind.DAY_TIME);
        assertThat(StructuredTemporalPreflightValidator.durationKind(schema, StructuredDuration.from(schema, 0, 0, 0, 0, 0, 0, 0)))
                .isEqualTo(StructuredDuration.Kind.DAY_TIME);
        assertThat(StructuredTemporalPreflightValidator.durationKind(schema, StructuredDuration.from(schema, 0, 0, 0, -838, -59, -59, 0)))
                .isEqualTo(StructuredDuration.Kind.DAY_TIME);
        assertThat(StructuredTemporalPreflightValidator.durationKind(schema, StructuredDuration.from(schema, 1, -2, 0, 0, 0, 0, 0)))
                .isEqualTo(StructuredDuration.Kind.YEAR_MONTH);
        // Days could be calendar days, whose length is not fixed at 24 hours
        assertThat(StructuredTemporalPreflightValidator.durationKind(schema, StructuredDuration.from(schema, 0, 0, 1, 0, 0, 0, 0)))
                .isEqualTo(StructuredDuration.Kind.MONTH_DAY_TIME);
        assertThat(StructuredTemporalPreflightValidator.durationKind(schema, StructuredDuration.from(schema, 1, 0, 0, 1, 0, 0, 0)))
                .isEqualTo(StructuredDuration.Kind.MONTH_DAY_TIME);
    }
}

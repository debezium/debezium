/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTimestamp;
import io.debezium.time.StructuredZonedTimestamp;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should bind structured timestamp infinity as PostgreSQL timestamp literal")
    void shouldBindStructuredTimestampInfinity() {
        final var schema = StructuredTimestamp.schema();

        final var bindings = StructuredTimestampType.INSTANCE.bind(1, schema, StructuredTimestamp.positiveInfinity(schema));

        assertInfinityBinding(bindings.get(0), "infinity");
        assertThat(StructuredTimestampType.INSTANCE.getQueryBinding(null, schema, null)).isEqualTo("cast(? as timestamp)");
    }

    @Test
    @DisplayName("Should bind structured zoned timestamp infinity as PostgreSQL timestamptz literal")
    void shouldBindStructuredZonedTimestampInfinity() {
        final var schema = StructuredZonedTimestamp.schema();

        final var bindings = StructuredZonedTimestampType.INSTANCE.bind(2, schema, StructuredZonedTimestamp.negativeInfinity(schema));

        assertThat(bindings).hasSize(1);
        assertInfinityBinding(bindings.get(0), "-infinity");
        assertThat(StructuredZonedTimestampType.INSTANCE.getQueryBinding(null, schema, null)).isEqualTo("cast(? as timestamptz)");
    }

    @Test
    @DisplayName("Should bind structured date infinity as PostgreSQL date literal")
    void shouldBindStructuredDateInfinity() {
        final var schema = StructuredDate.schema();

        final var bindings = StructuredDateType.INSTANCE.bind(3, schema, StructuredDate.positiveInfinity(schema));

        assertThat(bindings).hasSize(1);
        assertInfinityBinding(bindings.get(0), "infinity");
        assertThat(StructuredDateType.INSTANCE.getQueryBinding(null, schema, null)).isEqualTo("cast(? as date)");
    }

    @Test
    @DisplayName("Should bind structured duration as PostgreSQL interval literal")
    void shouldBindStructuredDuration() {
        final var schema = StructuredDuration.schema();
        final var value = StructuredDuration.from(schema, 1, 2, 3, 4, 5, 6, 789_000_000);

        final var bindings = StructuredDurationType.INSTANCE.bind(4, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getIndex()).isEqualTo(4);
        assertThat(bindings.get(0).getValue()).isEqualTo("1 years 2 months 3 days 4 hours 5 minutes 6.789 seconds");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
        assertThat(StructuredDurationType.INSTANCE.getQueryBinding(null, schema, value)).isEqualTo("cast(? as interval)");
    }

    private void assertInfinityBinding(ValueBindDescriptor binding, String expectedValue) {
        assertThat(binding.getValue()).isEqualTo(expectedValue);
        assertThat(binding.getTargetSqlType()).isEqualTo(Types.VARCHAR);
    }
}

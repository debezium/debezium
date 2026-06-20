/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.time.StructuredDate;
import io.debezium.time.StructuredDuration;
import io.debezium.time.StructuredTimestamp;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should bind zero date components as MySQL date literal")
    void shouldBindZeroDateComponents() {
        final var schema = StructuredDate.schema();
        final var value = StructuredDate.from(schema, 0, 0, 0);

        final var bindings = StructuredDateType.INSTANCE.bind(1, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("0000-00-00");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
        assertThat(StructuredDateType.INSTANCE.getDefaultValueBinding(schema, value)).isEqualTo("'0000-00-00'");
    }

    @Test
    @DisplayName("Should bind invalid date components as MySQL timestamp literal")
    void shouldBindInvalidTimestampComponents() {
        final var schema = StructuredTimestamp.schema();
        final var value = StructuredTimestamp.from(schema, 2026, 2, 31, 12, 13, 14, 123_456_000);

        final var bindings = StructuredTimestampType.INSTANCE.bind(2, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("2026-02-31 12:13:14.123456");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
        assertThat(StructuredTimestampType.INSTANCE.getDefaultValueBinding(schema, value)).isEqualTo("'2026-02-31 12:13:14.123456'");
    }

    @Test
    @DisplayName("Should bind structured duration as MySQL time literal")
    void shouldBindStructuredDuration() {
        final var schema = StructuredDuration.schema();
        final var value = StructuredDuration.from(schema, 0, 0, 0, -838, -59, -58, -999_999_000);

        final var bindings = StructuredDurationType.INSTANCE.bind(3, schema, value);

        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("-838:59:58.999999");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
        assertThat(StructuredDurationType.INSTANCE.getDefaultValueBinding(schema, value)).isEqualTo("'-838:59:58.999999'");
    }
}

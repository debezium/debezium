/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;

import org.hibernate.engine.jdbc.Size;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.StructuredDurationType;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.time.StructuredDuration;

@Tag("UnitTests")
class StructuredTemporalTypeTest {

    @Test
    @DisplayName("Should bind structured duration as Db2 string value")
    void shouldBindStructuredDuration() {
        final var schema = StructuredDuration.schema();
        final var value = StructuredDuration.from(schema, 1, 2, 3, 4, 5, 6, 789_000_000);
        final var type = new StructuredDurationType();
        type.configure(mock(SinkConnectorConfig.class), db2Dialect());

        final var bindings = type.bind(4, schema, value);

        assertThat(type.getTypeName(schema, false)).isEqualTo("varchar(128)");
        assertThat(type.getQueryBinding(null, schema, value)).isEqualTo("?");
        assertThat(type.getDefaultValueBinding(schema, value))
                .isEqualTo("'1 years 2 months 3 days 4 hours 5 minutes 6.789 seconds'");
        assertThat(bindings).hasSize(1);
        assertThat(bindings.get(0).getValue()).isEqualTo("1 years 2 months 3 days 4 hours 5 minutes 6.789 seconds");
        assertThat(bindings.get(0).getTargetSqlType()).isEqualTo(Types.VARCHAR);
    }

    private DatabaseDialect db2Dialect() {
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        when(dialect.getJdbcTypeName(eq(Types.VARCHAR), any(Size.class))).thenReturn("varchar(128)");
        return dialect;
    }
}

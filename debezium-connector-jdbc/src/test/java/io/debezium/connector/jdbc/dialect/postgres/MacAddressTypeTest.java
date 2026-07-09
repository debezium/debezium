/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for the PostgreSQL {@link MacAddressType} handler.
 */
@Tag("UnitTests")
class MacAddressTypeTest {

    private final MacAddressType type = MacAddressType.INSTANCE;

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should register for both macaddr and macaddr8 source column types")
    void shouldRegisterForMacAddressSourceColumnTypes() {
        assertThat(type.getRegistrationKeys()).containsExactly("MACADDR", "MACADDR8");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should resolve type name and query binding to macaddr")
    void shouldResolveMacaddr() {
        final Schema schema = SchemaBuilder.string()
                .parameter("__debezium.source.column.type", "MACADDR")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("MACADDR");
        assertThat(type.getQueryBinding(null, schema, "08:00:2b:01:02:03")).isEqualTo("cast(? as MACADDR)");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should resolve type name and query binding to macaddr8")
    void shouldResolveMacaddr8() {
        final Schema schema = SchemaBuilder.string()
                .parameter("__debezium.source.column.type", "MACADDR8")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("MACADDR8");
        assertThat(type.getQueryBinding(null, schema, "08:00:2b:01:02:03:04:05")).isEqualTo("cast(? as MACADDR8)");
    }

    @Test
    @FixFor("debezium/dbz#2100")
    @DisplayName("Should fall back to macaddr when the source column type is not propagated")
    void shouldFallBackToMacaddrWithoutSourceColumnType() {
        final Schema schema = SchemaBuilder.string().build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("macaddr");
        assertThat(type.getQueryBinding(null, schema, "08:00:2b:01:02:03")).isEqualTo("cast(? as macaddr)");
    }

}

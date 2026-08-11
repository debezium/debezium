/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for the MySQL {@link TinyIntType} handler.
 */
@Tag("UnitTests")
class TinyIntTypeTest {

    private final TinyIntType type = TinyIntType.INSTANCE;

    @Test
    @FixFor("debezium/dbz#2352")
    @DisplayName("Should resolve a propagated INT8 TINYINT to MySQL tinyint")
    void shouldResolveInt8PropagatedTinyIntToTinyInt() {
        final Schema schema = SchemaBuilder.int8()
                .parameter("__debezium.source.column.type", "TINYINT")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("tinyint");
    }

    @Test
    @FixFor("debezium/dbz#2352")
    @DisplayName("Should widen a propagated INT16 TINYINT to MySQL smallint")
    void shouldWidenInt16PropagatedTinyIntToSmallInt() {
        final Schema schema = SchemaBuilder.int16()
                .parameter("__debezium.source.column.type", "TINYINT")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("smallint");
    }

    @Test
    @FixFor("debezium/dbz#2352")
    @DisplayName("Should keep the display width for a propagated INT8 tinyint(n)")
    void shouldKeepDisplayWidthForInt8TinyInt() {
        final Schema schema = SchemaBuilder.int8()
                .parameter("__debezium.source.column.type", "TINYINT")
                .parameter("__debezium.source.column.length", "2")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("tinyint(2)");
    }

    @Test
    @FixFor("debezium/dbz#2352")
    @DisplayName("Should keep an INT16 tinyint that has a display width as tinyint(n), e.g. a BOOLEAN")
    void shouldKeepDisplayWidthForInt16TinyInt() {
        // A MySQL BOOLEAN mapped to INT16 propagates TINYINT with length 1; the display width must
        // win over the INT16 widening so it is not turned into smallint.
        final Schema schema = SchemaBuilder.int16()
                .parameter("__debezium.source.column.type", "TINYINT")
                .parameter("__debezium.source.column.length", "1")
                .build();

        assertThat(type.getTypeName(schema, false)).isEqualTo("tinyint(1)");
    }
}

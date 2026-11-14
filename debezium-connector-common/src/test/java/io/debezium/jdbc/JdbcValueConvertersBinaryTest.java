/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Tests that binary values delivered as {@code char[]} convert the same way as the equivalent {@link String}.
 */
public class JdbcValueConvertersBinaryTest {

    // Long enough that the UTF-8 encoder's backing array is larger than the encoded content
    private static final String VALUE = "helloworld-0123456789";

    private final Column column = Column.editor().name("c1").type("VARBINARY").optional(false).jdbcType(Types.VARBINARY).create();

    @Test
    @FixFor("debezium/dbz#3098")
    public void shouldConvertCharArrayToHexLikeString() {
        final var converter = converterFor(BinaryHandlingMode.HEX);
        assertThat(converter.convert(VALUE.toCharArray())).isEqualTo(converter.convert(VALUE));
    }

    @Test
    @FixFor("debezium/dbz#3098")
    public void shouldConvertCharArrayToBase64LikeString() {
        final var converter = converterFor(BinaryHandlingMode.BASE64);
        assertThat(converter.convert(VALUE.toCharArray())).isEqualTo(converter.convert(VALUE));
    }

    @Test
    @FixFor("debezium/dbz#3098")
    public void shouldConvertCharArrayToBase64UrlSafeLikeString() {
        final var converter = converterFor(BinaryHandlingMode.BASE64_URL_SAFE);
        assertThat(converter.convert(VALUE.toCharArray())).isEqualTo(converter.convert(VALUE));
    }

    private ValueConverter converterFor(BinaryHandlingMode mode) {
        final var converters = new JdbcValueConverters(null, TemporalPrecisionMode.ADAPTIVE, ZoneOffset.UTC, null, null, mode);
        final var field = new Field(column.name(), -1, converters.schemaBuilder(column).build());
        return converters.converter(column, field);
    }
}

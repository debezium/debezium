/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * @author Randall Hauch
 *
 */
public class TruncateStringsTest {

    private final Column column = Column.editor().name("col").create();
    private ValueConverter converter;

    @Test
    public void shouldTruncateStrings() {
        converter = new TruncateStrings(5).create(column);
        assertThat(converter.convert("1234567890").toString()).isEqualTo("12345");
        assertThat(converter.convert("123456").toString()).isEqualTo("12345");
        assertThat(converter.convert("12345").toString()).isEqualTo("12345");
        assertThat(converter.convert("1234").toString()).isEqualTo("1234");
        assertThat(converter.convert("123").toString()).isEqualTo("123");
        assertThat(converter.convert("12").toString()).isEqualTo("12");
        assertThat(converter.convert("1").toString()).isEqualTo("1");
        assertThat(converter.convert(null)).isNull();
    }

}

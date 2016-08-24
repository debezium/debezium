/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import java.sql.Types;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * @author Randall Hauch
 *
 */
public class MaskStringsTest {

    private final Column column = Column.editor().name("col").jdbcType(Types.VARCHAR).create();
    private ValueConverter converter;
    
    @Test
    public void shouldTruncateStrings() {
        String maskValue = "*****";
        converter = new MaskStrings(maskValue).create(column);
        assertThat(converter.convert("1234567890").toString()).isEqualTo(maskValue);
        assertThat(converter.convert("123456").toString()).isEqualTo(maskValue);
        assertThat(converter.convert("12345").toString()).isEqualTo(maskValue);
        assertThat(converter.convert("1234").toString()).isEqualTo(maskValue);
        assertThat(converter.convert("123").toString()).isEqualTo(maskValue);
        assertThat(converter.convert("12").toString()).isEqualTo(maskValue);
        assertThat(converter.convert("1").toString()).isEqualTo(maskValue);
        assertThat(converter.convert(null).toString()).isEqualTo(maskValue);
    }

}

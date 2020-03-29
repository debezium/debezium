/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.Types;

import org.junit.Test;

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
    public void shouldMaskStringsWithAsterisks() {
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

    @Test
    public void shouldTransformSameInputsToSameResultsForCharsetType() {
        converter = new MaskStrings("salt".getBytes(), "SHA-256").create(column);
        assertThat(converter.convert("hello")).isEqualTo("af5843a0f0e728ab0332c8888b6e1190bfb79e584f0d40538de8f10df6ef29c6");
        assertThat(converter.convert("hello")).isEqualTo("af5843a0f0e728ab0332c8888b6e1190bfb79e584f0d40538de8f10df6ef29c6");
        assertThat(converter.convert("world")).isEqualTo("4588e1f2dcdc7fefc1515d3acd5acb9033478eace68286f383c337b9ff4464a3");
        assertThat(converter.convert("world")).isEqualTo("4588e1f2dcdc7fefc1515d3acd5acb9033478eace68286f383c337b9ff4464a3");
    }

}

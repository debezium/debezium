/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import org.junit.Test;

import java.sql.Types;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
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
        assertThat(converter.convert("hello")).isEqualTo("cd31b3b98ece60cb739c0bf770b2de892ae0ad133f645513c3d83f08757a843a");
        assertThat(converter.convert("hello")).isEqualTo("cd31b3b98ece60cb739c0bf770b2de892ae0ad133f645513c3d83f08757a843a");
        assertThat(converter.convert("world")).isEqualTo("e84ac3142870113ddc6710c06f76421befc8e8ca6de64e98d2993ed8d41f4085");
        assertThat(converter.convert("world")).isEqualTo("e84ac3142870113ddc6710c06f76421befc8e8ca6de64e98d2993ed8d41f4085");
    }

    @Test
    public void shouldTransformSameInputsToSameResultsForCharsetTypeWithMD5() {
        converter = new MaskStrings("salt".getBytes(), "MD5").create(column);
        assertThat(converter.convert("hello")).isEqualTo("06decc8b095724f80103712c235586be");
        assertThat(converter.convert("world")).isEqualTo("172c8e95398cc72ab5358ead6981e7e5");
    }
}
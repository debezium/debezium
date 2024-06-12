/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.Test;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * @author Randall Hauch
 *
 */
public class TruncateColumnTest {

    private final Column column = Column.editor().name("col").create();
    private ValueConverter converter;

    @Test
    public void shouldTruncateStrings() {
        converter = new TruncateColumn(5).create(column);
        assertThat(converter.convert("1234567890").toString()).isEqualTo("12345");
        assertThat(converter.convert("123456").toString()).isEqualTo("12345");
        assertThat(converter.convert("12345").toString()).isEqualTo("12345");
        assertThat(converter.convert("1234").toString()).isEqualTo("1234");
        assertThat(converter.convert("123").toString()).isEqualTo("123");
        assertThat(converter.convert("12").toString()).isEqualTo("12");
        assertThat(converter.convert("1").toString()).isEqualTo("1");
        assertThat(converter.convert(null)).isNull();

        converter = new TruncateColumn(0).create(column);
        assertThat(converter.convert("1234567890").toString()).isEqualTo("");
        assertThat(converter.convert("123456").toString()).isEqualTo("");
        assertThat(converter.convert("12345").toString()).isEqualTo("");
        assertThat(converter.convert("1234").toString()).isEqualTo("");
        assertThat(converter.convert("123").toString()).isEqualTo("");
        assertThat(converter.convert("12").toString()).isEqualTo("");
        assertThat(converter.convert("1").toString()).isEqualTo("");
        assertThat(converter.convert("").toString()).isEqualTo("");
        assertThat(converter.convert(null)).isNull();
    }

    @Test
    public void shouldTruncateByteBuffer() {
        converter = new TruncateColumn(3).create(column);
        ByteBuffer buffer5 = createBuffer(5);
        ByteBuffer buffer4 = createBuffer(4);
        ByteBuffer buffer3 = createBuffer(3);
        ByteBuffer buffer2 = createBuffer(2);
        ByteBuffer buffer1 = createBuffer(1);
        assertThat(converter.convert(buffer5)).isEqualTo(buffer3);
        assertThat(converter.convert(buffer4)).isEqualTo(buffer3);
        assertThat(converter.convert(buffer3)).isEqualTo(buffer3);
        assertThat(converter.convert(buffer2)).isEqualTo(buffer2);
        assertThat(converter.convert(buffer1)).isEqualTo(buffer1);
        assertThat(converter.convert(null)).isNull();

        converter = new TruncateColumn(0).create(column);
        ByteBuffer buffer0 = createBuffer(0);
        assertThat(converter.convert(buffer5)).isEqualTo(buffer0);
        assertThat(converter.convert(buffer4)).isEqualTo(buffer0);
        assertThat(converter.convert(buffer3)).isEqualTo(buffer0);
        assertThat(converter.convert(buffer2)).isEqualTo(buffer0);
        assertThat(converter.convert(buffer1)).isEqualTo(buffer0);
        assertThat(converter.convert(buffer0)).isEqualTo(buffer0);
        assertThat(converter.convert(null)).isNull();
    }

    private ByteBuffer createBuffer(int size) {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++) {
            buffer.put((byte) i);
        }
        buffer.position(0);
        return buffer;
    }

}

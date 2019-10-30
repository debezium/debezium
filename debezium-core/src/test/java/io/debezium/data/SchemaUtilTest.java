/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.doc.FixFor;

/**
 * Test for {@link SchemaUtil}.
 *
 * @author Andreas Bergmeier
 *
 */
public class SchemaUtilTest {

    @Test
    @FixFor("DBZ-759")
    public void correctlySerializesByteArray() {
        assertThat(SchemaUtil.asString(new byte[]{ 1, 3, 5, 7 })).isEqualTo("[1, 3, 5, 7]");
    }

    @Test
    @FixFor("DBZ-759")
    public void correctlySerializesByteBuffer() {
        final ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put(new byte[]{ 11, 13, 17 });
        assertThat(SchemaUtil.asString(buffer)).isEqualTo("[11, 13, 17]");
    }

    @Test
    @FixFor("DBZ-759")
    public void correctlySerializesStructWithByteArray() {
        Schema schema = SchemaBuilder.struct()
                .field("some_field", SchemaBuilder.bytes().build())
                .build();

        Struct struct = new Struct(schema).put("some_field", new byte[]{ 1, 3, 5, 7 });
        assertThat(SchemaUtil.asString(struct)).isEqualTo("{\"some_field\" : [1, 3, 5, 7]}");
    }
}

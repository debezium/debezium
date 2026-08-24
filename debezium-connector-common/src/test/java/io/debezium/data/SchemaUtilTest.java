/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    @Test
    public void correctlyEscapesJsonString() {
        String originalText = "This is a \"test\" string with a backslash \\ and a newline\n.";
        String escapedText = SchemaUtil.asString(originalText);
        assertThat(escapedText).isEqualTo("\"This is a \\\"test\\\" string with a backslash \\\\ and a newline\\n.\"");
    }

    @Test
    public void canParseEscapedJsonString() throws JsonProcessingException {
        var originalText = "This is a \"test\" string with a backslash \\ and a newline\n.";
        var escapedText = SchemaUtil.asString(originalText);
        var mapper = new ObjectMapper();
        var read = mapper.readValue(escapedText, String.class);
        assertThat(read).isEqualTo(originalText);
    }

    @Test
    @FixFor("debezium/dbz#2492")
    public void escapesStringsIdenticallyToObjectMapper() throws JsonProcessingException {
        var samples = new ArrayList<>(List.of(
                "",
                "plain",
                "quote\" backslash\\ slash/",
                "café ✓ 🚀",
                "lone surrogate: \ud800 end"));
        var controlChars = new StringBuilder();
        for (char c = 0; c < 0x20; c++) {
            controlChars.append(c);
        }
        samples.add(controlChars.toString());

        var mapper = new ObjectMapper();
        for (String sample : samples) {
            assertThat(SchemaUtil.asString(sample)).isEqualTo(mapper.writeValueAsString(sample));
        }
    }
}

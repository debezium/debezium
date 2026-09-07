/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for the {@link StructToJsonConverter} helper.
 */
@Tag("UnitTests")
class StructToJsonConverterTest {

    @Test
    @FixFor("debezium/dbz#2573")
    @DisplayName("Should serialize scalar, decimal, binary, and null fields")
    void testSerializesScalarFields() {
        final Schema schema = SchemaBuilder.struct()
                .field("sku", Schema.STRING_SCHEMA)
                .field("quantity", Schema.INT32_SCHEMA)
                .field("price", Decimal.schema(2))
                .field("payload", Schema.OPTIONAL_BYTES_SCHEMA)
                .field("missing", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Struct struct = new Struct(schema)
                .put("sku", "A")
                .put("quantity", 1)
                .put("price", new BigDecimal("1.25"))
                .put("payload", ByteBuffer.wrap(new byte[]{ 1, 2 }));

        assertThat(StructToJsonConverter.structToJsonString(struct))
                .isEqualTo("{\"sku\":\"A\",\"quantity\":1,\"price\":1.25,\"payload\":\"AQI=\",\"missing\":null}");
    }

    @Test
    @FixFor("debezium/dbz#2573")
    @DisplayName("Should serialize nested structs, arrays, and maps recursively")
    void testSerializesNestedValues() {
        final Schema profileSchema = SchemaBuilder.struct()
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema schema = SchemaBuilder.struct()
                .field("profile", profileSchema)
                .field("tags", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                .field("attributes", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA).optional().build())
                .build();
        final Struct struct = new Struct(schema)
                .put("profile", new Struct(profileSchema).put("name", "Alice"))
                .put("tags", List.of("a", "b"))
                .put("attributes", Map.of("height", 42));

        assertThat(StructToJsonConverter.structToJsonString(struct))
                .isEqualTo("{\"profile\":{\"name\":\"Alice\"},\"tags\":[\"a\",\"b\"],\"attributes\":{\"height\":42}}");
    }
}

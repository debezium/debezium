/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

/**
 * Tests of {@link StateBuilder}.
 *
 * @author Jiri Pechanec
 */
public class StateBuilderTest {

    private static final Schema ROW = SchemaBuilder.struct().name("row")
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("description", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();

    private static final Schema VALUE = SchemaBuilder.struct().name("value")
            .field("before", ROW)
            .field("after", ROW)
            .field("op", Schema.STRING_SCHEMA)
            .build();

    private static Struct row(final String name, final String description) {
        return new Struct(ROW).put("name", name).put("description", description);
    }

    private static Struct value(final Struct before, final Struct after) {
        return new Struct(VALUE).put("before", before).put("after", after).put("op", "u");
    }

    private static StateBuilder json(final String... paths) {
        return new StateBuilder(List.of(paths), StateFormat.JSON);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(final Object state) {
        return (Map<String, Object>) state;
    }

    private static Map<String, Object> built(final StateBuilder builder, final Struct value) {
        return asMap(builder.build(value));
    }

    @Test
    public void shouldBuildJsonStateKeyedByPathInConfiguredOrder() {
        final var state = json("after.description", "before.name", "op").build(value(row("old", "was"), row("new", "is")));

        assertThat(state).isInstanceOf(Map.class);
        assertThat(asMap(state)).containsExactly(
                Map.entry("after.description", "is"),
                Map.entry("before.name", "old"),
                Map.entry("op", "u"));
    }

    @Test
    public void shouldOmitMissingValues() {
        final var state = json("after.name", "after.description", "before.name").build(value(null, row("new", null)));

        assertThat(asMap(state)).containsExactly(Map.entry("after.name", "new"));
    }

    @Test
    public void shouldReturnNullWhenAllValuesAreMissing() {
        assertThat(json("after.name", "after.description").build(value(row("old", "was"), null))).isNull();
    }

    @Test
    public void shouldBuildTextState() {
        final var state = new StateBuilder(List.of("after.name", "after.description", "before.name"), StateFormat.TEXT)
                .build(value(row(null, null), row("new", "is")));

        assertThat(state).isEqualTo("new\nis");
    }

    @Test
    public void shouldRejectUnknownField() {
        assertThatThrownBy(() -> json("after.unknown").build(value(null, row("a", "b")))).isInstanceOf(DataException.class).hasMessageContaining("after.unknown");
        assertThatThrownBy(() -> json("nothing.name").build(value(null, row("a", "b")))).isInstanceOf(DataException.class).hasMessageContaining("nothing.name");
    }

    @Test
    public void shouldRejectNonStructIntermediate() {
        assertThatThrownBy(() -> json("op.name").build(value(null, row("a", "b")))).isInstanceOf(DataException.class).hasMessageContaining("op.name");
    }

    private static Struct single(final Schema fieldSchema, final Object fieldValue) {
        final var schema = SchemaBuilder.struct().field("f", fieldSchema).build();
        return new Struct(schema).put("f", fieldValue);
    }

    @Test
    public void shouldConvertPrimitiveTypes() {
        assertThat(built(json("f"), single(Schema.STRING_SCHEMA, "text"))).containsEntry("f", "text");
        assertThat(built(json("f"), single(Schema.INT8_SCHEMA, (byte) 1))).containsEntry("f", (byte) 1);
        assertThat(built(json("f"), single(Schema.INT16_SCHEMA, (short) 2))).containsEntry("f", (short) 2);
        assertThat(built(json("f"), single(Schema.INT32_SCHEMA, 3))).containsEntry("f", 3);
        assertThat(built(json("f"), single(Schema.INT64_SCHEMA, 4L))).containsEntry("f", 4L);
        assertThat(built(json("f"), single(Schema.FLOAT32_SCHEMA, 1.5f))).containsEntry("f", 1.5f);
        assertThat(built(json("f"), single(Schema.FLOAT64_SCHEMA, 2.5d))).containsEntry("f", 2.5d);
        assertThat(built(json("f"), single(Schema.BOOLEAN_SCHEMA, true))).containsEntry("f", true);
    }

    @Test
    public void shouldRejectNonFiniteNumbers() {
        assertThatThrownBy(() -> json("f").build(single(Schema.FLOAT64_SCHEMA, Double.NaN))).isInstanceOf(DataException.class).hasMessageContaining("finite");
        assertThatThrownBy(() -> json("f").build(single(Schema.FLOAT32_SCHEMA, Float.POSITIVE_INFINITY))).isInstanceOf(DataException.class);
    }

    @Test
    public void shouldRejectBytes() {
        assertThatThrownBy(() -> json("f").build(single(Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{ 1 })))).isInstanceOf(DataException.class)
                .hasMessageContaining("bytes");
    }

    @Test
    public void shouldConvertLogicalTypes() {
        assertThat(built(json("f"), single(Decimal.schema(2), new BigDecimal("12.30")))).containsEntry("f", "12.30");
        assertThat(built(json("f"), single(org.apache.kafka.connect.data.Date.SCHEMA, new Date(86_400_000L * 365)))).containsEntry("f", "1971-01-01");
        assertThat(built(json("f"), single(Time.SCHEMA, new Date(3_723_000L)))).containsEntry("f", "01:02:03");
        assertThat(built(json("f"), single(Timestamp.SCHEMA, new Date(0L)))).containsEntry("f", "1970-01-01T00:00:00Z");
    }

    @Test
    public void shouldConvertNestedStructArrayAndMap() {
        final var inner = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).field("b", Schema.OPTIONAL_STRING_SCHEMA).build();
        final var schema = SchemaBuilder.struct()
                .field("s", inner)
                .field("l", SchemaBuilder.array(inner).build())
                .field("m", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .build();
        final var struct = new Struct(schema)
                .put("s", new Struct(inner).put("a", "x"))
                .put("l", List.of(new Struct(inner).put("a", "y").put("b", "z")))
                .put("m", Map.of("k", 7));

        final var state = built(json("s", "l", "m"), struct);

        assertThat(state.get("s")).isEqualTo(Map.of("a", "x"));
        assertThat(state.get("l")).isEqualTo(List.of(Map.of("a", "y", "b", "z")));
        assertThat(state.get("m")).isEqualTo(Map.of("k", 7));
    }

    @Test
    public void shouldRejectMapWithNonStringKeys() {
        final var schema = SchemaBuilder.struct().field("m", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build()).build();
        final var struct = new Struct(schema).put("m", Map.of(1, 2));

        assertThatThrownBy(() -> json("m").build(struct)).isInstanceOf(DataException.class).hasMessageContaining("string keys");
    }

    @Test
    public void shouldRenderComplexValuesAsJsonInTextState() {
        final var schema = SchemaBuilder.struct().field("m", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()).field("n", Schema.INT32_SCHEMA)
                .build();
        final var struct = new Struct(schema).put("m", Map.of("k", 7)).put("n", 5);

        assertThat(new StateBuilder(List.of("m", "n"), StateFormat.TEXT).build(struct)).isEqualTo("{\"k\":7}\n5");
    }
}

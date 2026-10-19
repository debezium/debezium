/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.data.Json;

/**
 * Unit tests for the {@link StringifyFields} transformation, which serializes selected struct or
 * array fields to a JSON string so that they can feed a schema-flexible column while sibling scalar
 * fields remain strongly typed.
 */
class StringifyFieldsTest {

    private final StringifyFields<SourceRecord> smt = new StringifyFields<>();

    @AfterEach
    void close() {
        smt.close();
    }

    private SourceRecord recordWith(Schema valueSchema, Struct value) {
        return new SourceRecord(null, null, "topic", 0, null, null, valueSchema, value);
    }

    /** Value schema: id INT32, props STRUCT{a INT32, b STRING}, name STRING. */
    private Schema valueSchemaWithProps() {
        Schema props = SchemaBuilder.struct().name("props")
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.STRING_SCHEMA)
                .build();
        return SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("props", props)
                .field("name", Schema.STRING_SCHEMA)
                .build();
    }

    @Test
    void serializesStructFieldToJsonStringAndRetypesToString() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "props"));
        Schema schema = valueSchemaWithProps();
        Struct props = new Struct(schema.field("props").schema()).put("a", 1).put("b", "x");
        Struct value = new Struct(schema).put("id", 7).put("props", props).put("name", "n");

        SourceRecord out = smt.apply(recordWith(schema, value));
        Struct outValue = (Struct) out.value();

        // The target field is now a STRING holding the JSON, while its siblings keep their types.
        assertThat(out.valueSchema().field("props").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(out.valueSchema().field("id").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat((String) outValue.get("props")).isEqualTo("{\"a\":1,\"b\":\"x\"}");
        assertThat(outValue.get("id")).isEqualTo(7);
        assertThat(outValue.get("name")).isEqualTo("n");
    }

    @Test
    void rejectsAMissingFieldsConfigurationAtConfigureTime() {
        // SmtManager validates the required option during the configuration pass, so a deployment that
        // forgets it fails while the connector is being set up rather than on the first record.
        assertThatThrownBy(() -> smt.configure(Map.of()))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void acceptsCommaSeparatedStringConfig() {
        // Some runtimes pass transformation config values as raw strings rather than parsing them
        // through ConfigDef.
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "props,tags"));
        final Schema schema = SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("props", SchemaBuilder.struct().name("props").field("a", Schema.INT32_SCHEMA).build())
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .build();
        final Struct props = new Struct(schema.field("props").schema()).put("a", 2);
        final Struct value = new Struct(schema).put("id", 1).put("props", props).put("tags", List.of("x", "y"));

        final SourceRecord out = smt.apply(recordWith(schema, value));
        final Struct outValue = (Struct) out.value();

        // Both configured fields are serialized and retyped.
        assertThat(out.valueSchema().field("props").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(out.valueSchema().field("tags").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat((String) outValue.get("props")).isEqualTo("{\"a\":2}");
        assertThat((String) outValue.get("tags")).isEqualTo("[\"x\",\"y\"]");
        assertThat(outValue.get("id")).isEqualTo(1);
    }

    @Test
    void serializesArrayFieldToJsonString() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "items"));
        final Schema item = SchemaBuilder.struct().name("item").field("a", Schema.INT32_SCHEMA).build();
        final Schema schema = SchemaBuilder.struct().name("Value")
                .field("items", SchemaBuilder.array(item).build())
                .build();
        final Struct value = new Struct(schema).put("items", List.of(new Struct(item).put("a", 1), new Struct(item).put("a", 2)));

        final SourceRecord out = smt.apply(recordWith(schema, value));

        assertThat(out.valueSchema().field("items").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat((String) ((Struct) out.value()).get("items")).isEqualTo("[{\"a\":1},{\"a\":2}]");
    }

    @Test
    void serializesMapFieldToJsonString() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "attributes"));
        final Schema schema = SchemaBuilder.struct().name("Value")
                .field("attributes", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .build();
        final Struct value = new Struct(schema).put("attributes", Map.of("k", 1));

        final SourceRecord out = smt.apply(recordWith(schema, value));

        assertThat(out.valueSchema().field("attributes").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat((String) ((Struct) out.value()).get("attributes")).isEqualTo("{\"k\":1}");
    }

    @Test
    void serializesNestedDecimalAsJsonNumber() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "props"));
        final Schema props = SchemaBuilder.struct().name("props").field("amount", Decimal.schema(2)).build();
        final Schema schema = SchemaBuilder.struct().name("Value").field("props", props).build();
        final Struct value = new Struct(schema).put("props", new Struct(props).put("amount", new BigDecimal("12.34")));

        final Struct out = (Struct) smt.apply(recordWith(schema, value)).value();

        assertThat((String) out.get("props")).isEqualTo("{\"amount\":12.34}");
    }

    @Test
    void keepsTheSchemaOfATargetFieldThatIsAlreadyAString() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "doc,props"));
        final Schema docSchema = Json.builder().optional().parameter("__debezium.source.column.type", "JSONB").defaultValue("{}").build();
        final Schema props = SchemaBuilder.struct().name("props").field("a", Schema.INT32_SCHEMA).build();
        final Schema schema = SchemaBuilder.struct().name("Value")
                .field("doc", docSchema)
                .field("props", props)
                .build();
        final Struct value = new Struct(schema).put("doc", "{\"x\":1}").put("props", new Struct(props).put("a", 1));

        final SourceRecord out = smt.apply(recordWith(schema, value));

        // The string field keeps its logical name, parameters and default, and its value passes through.
        assertThat(out.valueSchema().field("doc").schema()).isSameAs(docSchema);
        assertThat(((Struct) out.value()).get("doc")).isEqualTo("{\"x\":1}");
        assertThat((String) ((Struct) out.value()).get("props")).isEqualTo("{\"a\":1}");
    }

    @Test
    void returnsTheRecordUnchangedWhenNoConfiguredPathApplies() {
        // A heartbeat or an event from another topic has no field the configured paths address.
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "after.payload,doc"));
        final Schema schema = SchemaBuilder.struct().name("Heartbeat")
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("doc", Schema.STRING_SCHEMA)
                .build();
        final SourceRecord record = recordWith(schema, new Struct(schema).put("ts_ms", 1L).put("doc", "{}"));

        assertThat(smt.apply(record)).isSameAs(record);
    }

    @Test
    void reusesTheRetypedSchemaAcrossRecords() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "props"));
        final Schema schema = valueSchemaWithProps();
        final Struct props = new Struct(schema.field("props").schema()).put("a", 1).put("b", "x");

        final SourceRecord first = smt.apply(recordWith(schema, new Struct(schema).put("id", 1).put("props", props).put("name", "a")));
        final SourceRecord second = smt.apply(recordWith(schema, new Struct(schema).put("id", 2).put("props", props).put("name", "b")));

        assertThat(second.valueSchema()).isSameAs(first.valueSchema());
    }

    @Test
    void throwsWithoutTheValueWhenAFieldCannotBeSerialized() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "numbers"));
        final Schema schema = SchemaBuilder.struct().name("Value")
                .field("numbers", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                .build();
        final List<Object> numbers = new ArrayList<>(List.of(1));
        final Struct value = new Struct(schema).put("numbers", numbers);
        // The list is validated when it is put into the struct, so an element of the wrong type added
        // afterwards only fails when the field is serialized.
        numbers.add("secret");

        assertThatThrownBy(() -> smt.apply(recordWith(schema, value)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("numbers")
                .hasMessageContaining("ARRAY")
                .hasMessageNotContaining("secret");
    }

    @Test
    void acceptsListConfig() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, List.of("props")));
        Schema schema = valueSchemaWithProps();
        Struct props = new Struct(schema.field("props").schema()).put("a", 3).put("b", "w");
        Struct value = new Struct(schema).put("id", 1).put("props", props).put("name", "q");

        Struct out = (Struct) smt.apply(recordWith(schema, value)).value();
        assertThat((String) out.get("props")).isEqualTo("{\"a\":3,\"b\":\"w\"}");
    }

    @Test
    void leavesUntargetedFieldsUntouched() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "props"));
        Schema schema = valueSchemaWithProps();
        Struct props = new Struct(schema.field("props").schema()).put("a", 1).put("b", "x");
        Struct value = new Struct(schema).put("id", 42).put("props", props).put("name", "keep");

        SourceRecord out = smt.apply(recordWith(schema, value));
        // Untargeted fields keep their original schema types.
        assertThat(out.valueSchema().field("name").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(((Struct) out.value()).get("name")).isEqualTo("keep");
    }

    @Test
    void passesThroughWhenTargetFieldIsNull() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "props"));
        Schema optionalProps = SchemaBuilder.struct().name("props")
                .field("a", Schema.INT32_SCHEMA).optional().build();
        Schema schema = SchemaBuilder.struct().name("Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("props", optionalProps)
                .build();
        Struct value = new Struct(schema).put("id", 1); // props is left null

        SourceRecord out = smt.apply(recordWith(schema, value));
        // The schema is still retyped to an optional STRING, and the value stays null.
        assertThat(out.valueSchema().field("props").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(((Struct) out.value()).get("props")).isNull();
    }

    @Test
    void passesThroughNonStructValue() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "props"));
        SourceRecord tombstone = recordWith(null, null);
        SourceRecord out = smt.apply(tombstone);
        assertThat(out.value()).isNull();
    }

    /** Envelope-like value schema: id INT32, after STRUCT{payload STRUCT{a INT32, b STRING}, tag STRING}. */
    private Schema envelopeSchema(boolean optionalAfter) {
        Schema payload = SchemaBuilder.struct().name("payload")
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.STRING_SCHEMA)
                .build();
        SchemaBuilder after = SchemaBuilder.struct().name("after")
                .field("payload", payload)
                .field("tag", Schema.STRING_SCHEMA);
        if (optionalAfter) {
            after.optional();
        }
        return SchemaBuilder.struct().name("Envelope")
                .field("id", Schema.INT32_SCHEMA)
                .field("after", after.build())
                .build();
    }

    @Test
    void serializesNestedFieldViaDotNotation() {
        // A dot path reaches into a struct and retypes only the leaf, without a prior flattening step.
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "after.payload"));
        Schema schema = envelopeSchema(false);
        Struct payload = new Struct(schema.field("after").schema().field("payload").schema()).put("a", 5).put("b", "x");
        Struct after = new Struct(schema.field("after").schema()).put("payload", payload).put("tag", "t");
        Struct value = new Struct(schema).put("id", 9).put("after", after);

        SourceRecord out = smt.apply(recordWith(schema, value));
        Schema outAfterSchema = out.valueSchema().field("after").schema();
        Struct outAfter = (Struct) ((Struct) out.value()).get("after");

        // Only the leaf is retyped to STRING; the containing struct and its siblings keep their types.
        assertThat(outAfterSchema.type()).isEqualTo(Schema.Type.STRUCT);
        assertThat(outAfterSchema.field("payload").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(outAfterSchema.field("tag").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat((String) outAfter.get("payload")).isEqualTo("{\"a\":5,\"b\":\"x\"}");
        assertThat(outAfter.get("tag")).isEqualTo("t");
        assertThat(((Struct) out.value()).get("id")).isEqualTo(9);
    }

    @Test
    void passesThroughWhenIntermediateStructIsNull() {
        // If a struct along the path is absent, there is nothing to serialize and the value stays null.
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "after.payload"));
        Schema schema = envelopeSchema(true);
        Struct value = new Struct(schema).put("id", 1); // after is left null

        SourceRecord out = smt.apply(recordWith(schema, value));
        Schema outAfterSchema = out.valueSchema().field("after").schema();

        // The nested schema is still rewritten (payload is now STRING) but the null value is preserved.
        assertThat(outAfterSchema.field("payload").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat(((Struct) out.value()).get("after")).isNull();
    }

    @Test
    void throwsWhenPathNavigatesIntoNonStruct() {
        // A path that descends past a scalar field is a configuration mistake surfaced on the record.
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "id.nope"));
        Schema schema = envelopeSchema(false);
        Struct after = new Struct(schema.field("after").schema())
                .put("payload", new Struct(schema.field("after").schema().field("payload").schema()).put("a", 1).put("b", "x"))
                .put("tag", "t");
        Struct value = new Struct(schema).put("id", 3).put("after", after);

        assertThatThrownBy(() -> smt.apply(recordWith(schema, value)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("id");
    }

    /** Change-event-like value schema, in which before and after share the record schema "Value". */
    private Schema sharedEnvelopeSchema() {
        final Schema payload = SchemaBuilder.struct().name("payload").field("a", Schema.INT32_SCHEMA).build();
        final Schema row = SchemaBuilder.struct().name("Value").optional()
                .field("id", Schema.INT32_SCHEMA)
                .field("payload", payload)
                .build();
        return SchemaBuilder.struct().name("Envelope")
                .field("before", row)
                .field("after", row)
                .field("op", Schema.STRING_SCHEMA)
                .build();
    }

    @Test
    void serializesTheSamePathUnderBeforeAndAfter() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "before.payload,after.payload"));
        final Schema schema = sharedEnvelopeSchema();
        final Schema row = schema.field("after").schema();
        final Struct before = new Struct(row).put("id", 1).put("payload", new Struct(row.field("payload").schema()).put("a", 1));
        final Struct after = new Struct(row).put("id", 1).put("payload", new Struct(row.field("payload").schema()).put("a", 2));
        final Struct value = new Struct(schema).put("before", before).put("after", after).put("op", "u");

        final SourceRecord out = smt.apply(recordWith(schema, value));
        final Struct outValue = (Struct) out.value();

        // Both copies of the shared schema are retyped the same way, so they stay equal.
        assertThat(out.valueSchema().field("before").schema()).isEqualTo(out.valueSchema().field("after").schema());
        assertThat(out.valueSchema().field("after").schema().field("payload").schema().type()).isEqualTo(Schema.Type.STRING);
        assertThat((String) ((Struct) outValue.get("before")).get("payload")).isEqualTo("{\"a\":1}");
        assertThat((String) ((Struct) outValue.get("after")).get("payload")).isEqualTo("{\"a\":2}");
    }

    @Test
    void throwsWhenOnlyOneCopyOfASharedSchemaIsRetyped() {
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "after.payload"));
        final Schema schema = sharedEnvelopeSchema();
        final Struct value = new Struct(schema).put("op", "d");

        assertThatThrownBy(() -> smt.apply(recordWith(schema, value)))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("'before'")
                .hasMessageContaining("'after'")
                .hasMessageContaining("'Value'");
    }

    @Test
    void rejectsEmptyPathSegmentAtConfigureTime() {
        // Like a missing option, an invalid path is reported by SmtManager during the configuration pass.
        assertThatThrownBy(() -> smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "after.")))
                .isInstanceOf(ConfigException.class);
        assertThatThrownBy(() -> smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "after..payload")))
                .isInstanceOf(ConfigException.class);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
        smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "props"));
        Schema schema = valueSchemaWithProps();
        Struct props = new Struct(schema.field("props").schema()).put("a", 2).put("b", "y");
        Struct value = new Struct(schema).put("id", 1).put("props", props).put("name", "z");

        Struct out = (Struct) smt.apply(recordWith(schema, value)).value();
        assertThat((String) out.get("props")).isEqualTo("{\"a\":2,\"b\":\"y\"}");
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

    @Test
    void rejectsEmptyPathSegmentAtConfigureTime() {
        assertThatThrownBy(() -> smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "after.")))
                .isInstanceOf(ConnectException.class);
        assertThatThrownBy(() -> smt.configure(Map.of(StringifyFields.FIELDS_CONFIG, "after..payload")))
                .isInstanceOf(ConnectException.class);
    }
}

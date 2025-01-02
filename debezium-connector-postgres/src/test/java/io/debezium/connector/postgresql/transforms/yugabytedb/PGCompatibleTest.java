package io.debezium.connector.postgresql.transforms.yugabytedb;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.ADD_HEADERS;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.HANDLE_DELETES;

/**
 * Tests for {@link PGCompatible}
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class PGCompatibleTest {
    final Schema idSchema =  SchemaBuilder.struct()
            .field("value", Schema.INT64_SCHEMA)
            .field("set", Schema.BOOLEAN_SCHEMA);

    final Schema nameSchema =  SchemaBuilder.struct()
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .field("set", Schema.BOOLEAN_SCHEMA)
            .optional();

    final Schema keySchema = SchemaBuilder.struct()
            .field("id", idSchema)
            .build();

    final Schema valueSchema = SchemaBuilder.struct()
            .field("id", idSchema)
            .field("name", nameSchema)
            .field("location", nameSchema).optional()
            .build();

    final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .field("op", Schema.STRING_SCHEMA)
            .build();

    final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(valueSchema)
            .withSource(sourceSchema)
            .build();

    private Struct createIdStruct() {
        final Struct id = new Struct(idSchema);
        id.put("value", 1L);
        id.put("set", true);

        return id;
    }

    private Struct createNameStruct() {
        final Struct name = new Struct(nameSchema);
        name.put("value", "yb");
        name.put("set", true);
        return name;
    }

    private Struct createLocationStruct() {
        final Struct name = new Struct(nameSchema);
        name.put("value", null);
        name.put("set", false);
        return name;
    }

    private Struct createValue() {
        final Struct value = new Struct(valueSchema);
        value.put("id", createIdStruct());
        value.put("name", createNameStruct());
        value.put("location", createLocationStruct());

        return value;
    }

    @Test
    public void testSingleLevelStruct() {
        try (final PGCompatible<SourceRecord> transform = new PGCompatible<>()) {
            final Pair<Schema, Struct> unwrapped = transform.getUpdatedValueAndSchema(valueSchema, createValue());
            Assert.assertEquals(1, (long) unwrapped.getSecond().getInt64("id"));
            Assert.assertEquals("yb", unwrapped.getSecond().getString("name"));
            Assert.assertNull(unwrapped.getSecond().getString("location"));
        }
    }

    private Struct createPayload() {
        final Struct source = new Struct(sourceSchema);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        source.put("op", "c");
        return envelope.create(createValue(), source, Instant.now());
    }

    @Test
    public void testPayload() {
        try (final PGCompatible<SourceRecord> transform = new PGCompatible<>()) {
            Struct payload = createPayload();
            final Pair<Schema, Struct> unwrapped = transform.getUpdatedValueAndSchema(payload.schema(), payload);
            Schema valueSchema = unwrapped.getFirst();

            Assert.assertSame(valueSchema.type(), Schema.Type.STRUCT);
            Assert.assertEquals(6, valueSchema.fields().size());
            Assert.assertSame(valueSchema.field("op").schema().type(), Schema.Type.STRING);

            Schema afterSchema = valueSchema.field("after").schema();
            Assert.assertSame(afterSchema.type(), Schema.Type.STRUCT);
            Assert.assertEquals(3, afterSchema.fields().size());
            Assert.assertSame(afterSchema.field("id").schema().type(), Schema.Type.INT64);
            Assert.assertSame(afterSchema.field("name").schema().type(), Schema.Type.STRING);
            Assert.assertSame(afterSchema.field("location").schema().type(), Schema.Type.STRING);

            Struct after = unwrapped.getSecond().getStruct("after");
            Assert.assertEquals(1, (long) after.getInt64("id"));
            Assert.assertEquals("yb", after.getString("name"));
        }
    }

    private SourceRecord createCreateRecord() {
        final Struct key = new Struct(keySchema);
        key.put("id", createIdStruct());

        final Struct payload = createPayload();
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), payload);
    }

    @Test
    public void testHandleCreateRewrite() {
        try (final PGCompatible<SourceRecord> transform = new PGCompatible<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES.name(), "rewrite");
            props.put(ADD_HEADERS.name(), "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            Struct after = ((Struct) unwrapped.value()).getStruct("after");
            Assert.assertEquals(1, (long) ((Struct) unwrapped.value()).getStruct("after").getInt64("id"));
            Assert.assertEquals("yb", ((Struct) unwrapped.value()).getStruct("after").getString("name"));

            Assert.assertEquals("c", ((Struct) unwrapped.value()).getString("op"));
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.data.Json;
import io.debezium.data.vector.DoubleVector;
import io.debezium.data.vector.FloatVector;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the {@link VectorToJsonConverter} transformation.
 *
 * @author Chris Cranford
 */
public class VectorToJsonConverterTest {

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().optional()
            .field("table", Schema.STRING_SCHEMA)
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    private static final Schema RECORD_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT8_SCHEMA)
            .field("vec1", DoubleVector.schema())
            .field("vec2", FloatVector.schema())
            .field("vec3", sparseVectorSchema(false))
            .field("vec4", DoubleVector.builder().optional().build())
            .field("vec5", FloatVector.builder().optional().build())
            .field("vec6", sparseVectorSchema(true))
            .build();

    private final VectorToJsonConverter<SourceRecord> converter = new VectorToJsonConverter<>();

    @Test
    @FixFor("DBZ-8571")
    public void testTransformFlattenedEvent() {
        final Map<String, String> properties = new HashMap<>();
        converter.configure(properties);

        final Struct payload = new Struct(RECORD_SCHEMA);
        payload.put("id", (byte) 1);
        payload.put("vec1", DoubleVector.fromLogical(DoubleVector.schema(), "[1,2,3]"));
        payload.put("vec2", FloatVector.fromLogical(FloatVector.schema(), "[10,20,30]"));
        payload.put("vec3", nmakeSparseVector(sparseVectorSchema(false), 25, Map.of((short) 1, 10.0, (short) 5, 20.0, (short) 10, 30.0)));

        final SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                payload.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);
        final Struct transformedValue = requireStruct(transformedRecord.value(), "value should be a struct");

        assertThat(transformedValue.schema().field("vec1").schema()).isEqualTo(Json.schema());
        assertThat(transformedValue.schema().field("vec2").schema()).isEqualTo(Json.schema());
        assertThat(transformedValue.schema().field("vec3").schema()).isEqualTo(Json.schema());
        assertThat(transformedValue.schema().field("vec4").schema()).isEqualTo(Json.builder().optional().build());
        assertThat(transformedValue.schema().field("vec5").schema()).isEqualTo(Json.builder().optional().build());
        assertThat(transformedValue.schema().field("vec6").schema()).isEqualTo(Json.builder().optional().build());

        assertThat(transformedValue.get("vec1")).isEqualTo("{ \"values\": [1.0, 2.0, 3.0] }");
        assertThat(transformedValue.get("vec2")).isEqualTo("{ \"values\": [10.0, 20.0, 30.0] }");
        assertThat(transformedValue.get("vec3")).isEqualTo("{ \"dimensions\": 25, \"vector\": { \"1\": 10.0, \"5\": 20.0, \"10\": 30.0 } }");
        assertThat(transformedValue.get("vec4")).isNull();
        assertThat(transformedValue.get("vec5")).isNull();
        assertThat(transformedValue.get("vec6")).isNull();
    }

    @Test
    @FixFor("DBZ-8571")
    public void testTransformDebeziumEvent() {
        final Map<String, String> properties = new HashMap<>();
        converter.configure(properties);

        final Struct after = new Struct(RECORD_SCHEMA);
        after.put("id", (byte) 1);
        after.put("vec1", DoubleVector.fromLogical(DoubleVector.schema(), "[1,2,3]"));
        after.put("vec2", FloatVector.fromLogical(FloatVector.schema(), "[10,20,30]"));
        after.put("vec3", nmakeSparseVector(sparseVectorSchema(false), 25, Map.of((short) 1, 10.0, (short) 5, 20.0, (short) 10, 30.0)));

        final Struct source = new Struct(SOURCE_SCHEMA);
        source.put("table", "vectors");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(RECORD_SCHEMA)
                .withSource(SOURCE_SCHEMA)
                .build();

        final Struct payload = envelope.create(after, source, Instant.now());

        final SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = (Struct) transformedRecord.value();
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.schema().field("vec1").schema()).isEqualTo(Json.schema());
        assertThat(transformedAfter.schema().field("vec2").schema()).isEqualTo(Json.schema());
        assertThat(transformedAfter.schema().field("vec3").schema()).isEqualTo(Json.schema());
        assertThat(transformedAfter.schema().field("vec4").schema()).isEqualTo(Json.builder().optional().build());
        assertThat(transformedAfter.schema().field("vec5").schema()).isEqualTo(Json.builder().optional().build());
        assertThat(transformedAfter.schema().field("vec6").schema()).isEqualTo(Json.builder().optional().build());

        assertThat(transformedAfter.get("vec1")).isEqualTo("{ \"values\": [1.0, 2.0, 3.0] }");
        assertThat(transformedAfter.get("vec2")).isEqualTo("{ \"values\": [10.0, 20.0, 30.0] }");
        assertThat(transformedAfter.get("vec3")).isEqualTo("{ \"dimensions\": 25, \"vector\": { \"1\": 10.0, \"5\": 20.0, \"10\": 30.0 } }");
        assertThat(transformedAfter.get("vec4")).isNull();
        assertThat(transformedAfter.get("vec5")).isNull();
        assertThat(transformedAfter.get("vec6")).isNull();
    }

    /**
     * Creates a SparseVector schema, defined inside the PostgreSQL connector.
     *
     * @param optional whether the field can be null
     */
    protected static Schema sparseVectorSchema(boolean optional) {
        final SchemaBuilder builder = SchemaBuilder.struct()
                .name("io.debezium.data.SparseVector")
                .version(1)
                .doc("Sparse vector")
                .field("dimensions", Schema.INT16_SCHEMA)
                .field("vector", SchemaBuilder.map(Schema.INT16_SCHEMA, Schema.FLOAT64_SCHEMA).build());
        if (optional) {
            builder.optional();
        }
        return builder.build();
    }

    protected static Struct nmakeSparseVector(Schema schema, int dimensions, Map<Short, Double> values) {
        final Struct value = new Struct(schema);
        value.put("dimensions", (short) dimensions);
        value.put("vector", values);
        return value;
    }
}

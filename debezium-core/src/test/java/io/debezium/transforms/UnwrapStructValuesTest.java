/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;

public class UnwrapStructValuesTest {

    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", Schema.INT8_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();

    final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .build();

    final Schema int8StructWrappedSchema = SchemaBuilder.struct()
            .field("value", Schema.INT8_SCHEMA)
            .field("deletion_ts", Schema.OPTIONAL_INT64_SCHEMA)
            .field("set", Schema.BOOLEAN_SCHEMA)
            .build();

    final Schema stringStructWrappedSchema = SchemaBuilder.struct()
            .field("value", Schema.STRING_SCHEMA)
            .field("deletion_ts", Schema.OPTIONAL_INT64_SCHEMA)
            .field("set", Schema.BOOLEAN_SCHEMA)
            .build();

    final Schema recordStructWrappedSchema = SchemaBuilder.struct()
            .field("id", int8StructWrappedSchema)
            .field("name", stringStructWrappedSchema)
            .build();

    final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(recordStructWrappedSchema)
            .withSource(sourceSchema)
            .build();

    private SourceRecord createRecord() {
        final Struct before = new Struct(recordStructWrappedSchema);
        final Struct after = new Struct(recordStructWrappedSchema);
        final Struct source = new Struct(sourceSchema);

        final Struct idBefore = new Struct(int8StructWrappedSchema);
        idBefore.put("value", (byte) 1);
        idBefore.put("set", true);

        final Struct nameBefore = new Struct(stringStructWrappedSchema);
        nameBefore.put("value", "myRecord");
        nameBefore.put("set", true);

        final Struct idAfter = new Struct(int8StructWrappedSchema);
        idAfter.put("value", (byte) 1);
        idAfter.put("set", true);

        final Struct nameAfter = new Struct(stringStructWrappedSchema);
        nameAfter.put("value", "updatedRecord");
        nameAfter.put("set", true);

        before.put("id", idBefore);
        before.put("name", nameBefore);
        after.put("id", idAfter);
        after.put("name", nameAfter);

        source.put("lsn", 1234);

        final Struct payload = envelope.update(before, after, source, Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelope.schema(), payload);
    }

    @Test
    public void testUnwrapCreateRecord() {
        try (final UnwrapStructValues<SourceRecord> transform = new UnwrapStructValues<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createRecord = createRecord();
            final Struct originalValue = (Struct) createRecord.value();
            final SourceRecord unwrapped = transform.apply(createRecord);
            final Struct transformedValue = (Struct) unwrapped.value();

            assertThat(transformedValue.schema().fields().size()).isEqualTo(originalValue.schema().fields().size());
            for (Field item : originalValue.schema().fields()) {
                if (item.name().equals("before") || item.name().equals("after")) {
                    // the schema and values are modified to represent the plain schema and values, respectively
                    assertThat(transformedValue.schema().field(item.name()).schema()).isEqualTo(recordSchema);
                    Struct istruct = transformedValue.getStruct(item.name());
                    assertThat(istruct.getInt8("id")).isEqualTo((byte) 1);
                    assertThat(istruct.getString("name")).isEqualTo(item.name().equals("before") ? "myRecord" : "updatedRecord");
                }
                else {
                    // the schema and values are unmodified
                    assertThat(transformedValue.schema().field(item.name()).schema()).isEqualTo(item.schema());
                    assertThat(transformedValue.get(item)).isEqualTo(originalValue.get(item));
                }
            }
        }
    }
}

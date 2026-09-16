/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;

/**
 * Unit test for the {@link ExtractChangedRecordState} single message transformation.
 *
 * @author Chris Cranford
 */
public class ExtractChangedRecordStateTest extends AbstractExtractStateTest {

    @Test
    @FixFor("DBZ-8721")
    public void testUpdateWithDefaultValues() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecordWithOptionalNull();
            // Update the name from null to the default value "default_str" instead of "updatedRecord"
            ((Struct) updatedRecord.value()).getStruct("after").put("name", "default_str");

            final SourceRecord transformRecord = transform.apply(updatedRecord);
            final Header changedHeader = getSourceRecordHeader(transformRecord, "Changed");
            final List<String> changedHeaderValues = (List<String>) changedHeader.value();
            assertThat(changedHeaderValues).containsExactly("name");
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testAddUpdatedFieldToHeaders() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            props.put("header.unchanged.name", "Unchanged");
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecord();
            final SourceRecord transformRecord = transform.apply(updatedRecord);
            final Header changedHeader = getSourceRecordHeader(transformRecord, "Changed");
            final List<String> changedHeaderValues = (List<String>) changedHeader.value();
            final Header unchangedHeader = getSourceRecordHeader(transformRecord, "Unchanged");
            final List<String> unchangedHeaderValues = (ArrayList<String>) unchangedHeader.value();

            assertThat(transformRecord.headers().size()).isEqualTo(2);
            assertThat(changedHeaderValues.size()).isEqualTo(1);
            assertThat(changedHeaderValues.get(0)).isEqualTo("name");
            assertThat(changedHeader.schema().name()).isEqualTo("Changed");
            assertThat(unchangedHeaderValues.size()).isEqualTo(1);
            assertThat(unchangedHeaderValues.get(0)).isEqualTo("id");
        }

        // Not set unchanged header
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecord();
            final SourceRecord transformRecord = transform.apply(updatedRecord);
            final Header changedHeader = getSourceRecordHeader(transformRecord, "Changed");
            final List<String> changedHeaderValues = (List<String>) changedHeader.value();
            final Header unchangedHeader = getSourceRecordHeader(transformRecord, "Unchanged");

            assertThat(transformRecord.headers().size()).isEqualTo(1);
            assertThat(changedHeaderValues.contains("name")).isTrue();
            assertThat(unchangedHeader).isNull();
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testAddCreatedFieldToHeaders() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);
            props.put("header.changed.name", "Changed");
            props.put("header.unchanged.name", "Unchanged");
            transform.configure(props);

            final SourceRecord createdRecord = createCreateRecord();
            final SourceRecord transformRecord = transform.apply(createdRecord);
            assertThat(transformRecord.headers().size()).isEqualTo(2);
            assertThat((List<?>) getSourceRecordHeader(transformRecord, "Changed").value()).isEmpty();
            assertThat((List<?>) getSourceRecordHeader(transformRecord, "Unchanged").value()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testNoHeadersSetWhenNotConfiguredWithCreate() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord createdRecord = createCreateRecord();
            final SourceRecord transformRecord = transform.apply(createdRecord);
            assertThat(transformRecord.headers()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testNoHeadersSetWhenNotConfiguredWithUpdate() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord updatedRecord = createUpdateRecord();
            final SourceRecord transformRecord = transform.apply(updatedRecord);
            assertThat(transformRecord.headers()).isEmpty();
        }
    }

    @Test
    @FixFor("DBZ-5283")
    public void testNoHeadersSetWhenNotConfiguredWithDelete() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            transform.configure(props);

            final SourceRecord deleteRecord = createDeleteRecord();
            final SourceRecord transformRecord = transform.apply(deleteRecord);
            assertThat(transformRecord.headers()).isEmpty();
        }
    }

    @Test
    @FixFor("debezium/dbz#2635")
    public void testBinaryFieldComparisonUnchangedAndChanged() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            props.put("header.unchanged.name", "Unchanged");
            transform.configure(props);

            final Schema binaryRecordSchema = SchemaBuilder.struct()
                    .field("id", Schema.INT8_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("data", Schema.BYTES_SCHEMA)
                    .build();

            final Envelope binaryEnvelope = Envelope.defineSchema()
                    .withName("dummy.Envelope")
                    .withRecord(binaryRecordSchema)
                    .withSource(sourceSchema)
                    .build();

            // Case 1: Unchanged binary field (new byte[] instance with identical content)
            Struct before1 = new Struct(binaryRecordSchema);
            before1.put("id", (byte) 1);
            before1.put("name", "myRecord");
            before1.put("data", new byte[]{ 1, 2, 3 });

            Struct after1 = new Struct(binaryRecordSchema);
            after1.put("id", (byte) 1);
            after1.put("name", "updatedRecord");
            after1.put("data", new byte[]{ 1, 2, 3 });

            Struct source = new Struct(sourceSchema);
            source.put("lsn", 1234);
            Struct payload1 = binaryEnvelope.update(before1, after1, source, Instant.now());
            SourceRecord record1 = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", binaryEnvelope.schema(), payload1);

            SourceRecord transformed1 = transform.apply(record1);
            List<String> changed1 = (List<String>) getSourceRecordHeader(transformed1, "Changed").value();
            List<String> unchanged1 = (List<String>) getSourceRecordHeader(transformed1, "Unchanged").value();

            assertThat(changed1).containsExactly("name");
            assertThat(unchanged1).containsExactlyInAnyOrder("id", "data");

            // Case 2: Changed binary field
            Struct before2 = new Struct(binaryRecordSchema);
            before2.put("id", (byte) 1);
            before2.put("name", "myRecord");
            before2.put("data", new byte[]{ 1, 2, 3 });

            Struct after2 = new Struct(binaryRecordSchema);
            after2.put("id", (byte) 1);
            after2.put("name", "myRecord");
            after2.put("data", new byte[]{ 4, 5, 6 });

            Struct payload2 = binaryEnvelope.update(before2, after2, source, Instant.now());
            SourceRecord record2 = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", binaryEnvelope.schema(), payload2);

            SourceRecord transformed2 = transform.apply(record2);
            List<String> changed2 = (List<String>) getSourceRecordHeader(transformed2, "Changed").value();
            List<String> unchanged2 = (List<String>) getSourceRecordHeader(transformed2, "Unchanged").value();

            assertThat(changed2).containsExactly("data");
            assertThat(unchanged2).containsExactlyInAnyOrder("id", "name");
        }
    }

    @Test
    @FixFor("debezium/dbz#2635")
    public void testSchemaEvolutionWithNewFieldInAfter() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            props.put("header.unchanged.name", "Unchanged");
            transform.configure(props);

            final Schema beforeSchema = SchemaBuilder.struct()
                    .field("id", Schema.INT8_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .build();

            final Schema afterSchema = SchemaBuilder.struct()
                    .field("id", Schema.INT8_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("new_col", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

            final Schema envelopeSchema = SchemaBuilder.struct()
                    .name("dummy.Envelope")
                    .field("before", beforeSchema)
                    .field("after", afterSchema)
                    .field("source", sourceSchema)
                    .field("op", Schema.STRING_SCHEMA)
                    .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                    .build();

            final Struct before = new Struct(beforeSchema);
            before.put("id", (byte) 1);
            before.put("name", "myRecord");

            final Struct after = new Struct(afterSchema);
            after.put("id", (byte) 1);
            after.put("name", "myRecord");
            after.put("new_col", "newValue");

            final Struct source = new Struct(sourceSchema);
            source.put("lsn", 1234);

            final Struct payload = new Struct(envelopeSchema);
            payload.put("before", before);
            payload.put("after", after);
            payload.put("source", source);
            payload.put("op", "u");

            final SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelopeSchema, payload);

            final SourceRecord transformedRecord = transform.apply(record);
            final List<String> changedValues = (List<String>) getSourceRecordHeader(transformedRecord, "Changed").value();
            final List<String> unchangedValues = (List<String>) getSourceRecordHeader(transformedRecord, "Unchanged").value();

            assertThat(changedValues).containsExactly("new_col");
            assertThat(unchangedValues).containsExactlyInAnyOrder("id", "name");
        }
    }

    @Test
    @FixFor("debezium/dbz#2635")
    public void testSchemaEvolutionWithDroppedFieldInAfter() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            props.put("header.unchanged.name", "Unchanged");
            transform.configure(props);

            final Schema beforeSchema = SchemaBuilder.struct()
                    .field("id", Schema.INT8_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("old_col", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

            final Schema afterSchema = SchemaBuilder.struct()
                    .field("id", Schema.INT8_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .build();

            final Schema envelopeSchema = SchemaBuilder.struct()
                    .name("dummy.Envelope")
                    .field("before", beforeSchema)
                    .field("after", afterSchema)
                    .field("source", sourceSchema)
                    .field("op", Schema.STRING_SCHEMA)
                    .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                    .build();

            final Struct before = new Struct(beforeSchema);
            before.put("id", (byte) 1);
            before.put("name", "myRecord");
            before.put("old_col", "oldValue");

            final Struct after = new Struct(afterSchema);
            after.put("id", (byte) 1);
            after.put("name", "myRecord");

            final Struct source = new Struct(sourceSchema);
            source.put("lsn", 1234);

            final Struct payload = new Struct(envelopeSchema);
            payload.put("before", before);
            payload.put("after", after);
            payload.put("source", source);
            payload.put("op", "u");

            final SourceRecord record = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelopeSchema, payload);

            final SourceRecord transformedRecord = transform.apply(record);
            final List<String> changedValues = (List<String>) getSourceRecordHeader(transformedRecord, "Changed").value();
            final List<String> unchangedValues = (List<String>) getSourceRecordHeader(transformedRecord, "Unchanged").value();

            assertThat(changedValues).containsExactly("old_col");
            assertThat(unchangedValues).containsExactlyInAnyOrder("id", "name");
        }
    }

    @Test
    @FixFor("debezium/dbz#2635")
    public void testListOfByteArraysComparisonUnchangedAndChanged() {
        try (ExtractChangedRecordState<SourceRecord> transform = new ExtractChangedRecordState<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put("header.changed.name", "Changed");
            props.put("header.unchanged.name", "Unchanged");
            transform.configure(props);

            final Schema binaryListRecordSchema = SchemaBuilder.struct()
                    .field("id", Schema.INT8_SCHEMA)
                    .field("name", Schema.STRING_SCHEMA)
                    .field("data", SchemaBuilder.array(Schema.BYTES_SCHEMA).build())
                    .build();

            final Envelope binaryListEnvelope = Envelope.defineSchema()
                    .withName("dummy.Envelope")
                    .withRecord(binaryListRecordSchema)
                    .withSource(sourceSchema)
                    .build();

            // Case 1: Unchanged list of byte arrays (new instances with identical content)
            Struct before1 = new Struct(binaryListRecordSchema);
            before1.put("id", (byte) 1);
            before1.put("name", "myRecord");
            before1.put("data", List.of(new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5, 6 }));

            Struct after1 = new Struct(binaryListRecordSchema);
            after1.put("id", (byte) 1);
            after1.put("name", "updatedRecord");
            after1.put("data", List.of(new byte[]{ 1, 2, 3 }, new byte[]{ 4, 5, 6 }));

            Struct source = new Struct(sourceSchema);
            source.put("lsn", 1234);
            Struct payload1 = binaryListEnvelope.update(before1, after1, source, Instant.now());
            SourceRecord record1 = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", binaryListEnvelope.schema(), payload1);

            SourceRecord transformed1 = transform.apply(record1);
            List<String> changed1 = (List<String>) getSourceRecordHeader(transformed1, "Changed").value();
            List<String> unchanged1 = (List<String>) getSourceRecordHeader(transformed1, "Unchanged").value();

            assertThat(changed1).containsExactly("name");
            assertThat(unchanged1).containsExactlyInAnyOrder("id", "data");

            // Case 2: Changed list of byte arrays
            Struct before2 = new Struct(binaryListRecordSchema);
            before2.put("id", (byte) 1);
            before2.put("name", "myRecord");
            before2.put("data", List.of(new byte[]{ 1, 2, 3 }));

            Struct after2 = new Struct(binaryListRecordSchema);
            after2.put("id", (byte) 1);
            after2.put("name", "myRecord");
            after2.put("data", List.of(new byte[]{ 7, 8, 9 }));

            Struct payload2 = binaryListEnvelope.update(before2, after2, source, Instant.now());
            SourceRecord record2 = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", binaryListEnvelope.schema(), payload2);

            SourceRecord transformed2 = transform.apply(record2);
            List<String> changed2 = (List<String>) getSourceRecordHeader(transformed2, "Changed").value();
            List<String> unchanged2 = (List<String>) getSourceRecordHeader(transformed2, "Unchanged").value();

            assertThat(changed2).containsExactly("data");
            assertThat(unchanged2).containsExactlyInAnyOrder("id", "name");
        }
    }

    private Header getSourceRecordHeader(SourceRecord record, String headerKey) {
        Iterator<Header> operationHeader = record.headers().allWithName(headerKey);
        if (!operationHeader.hasNext()) {
            return null;
        }
        return operationHeader.next();
    }
}

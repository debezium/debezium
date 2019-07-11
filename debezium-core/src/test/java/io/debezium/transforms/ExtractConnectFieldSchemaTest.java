/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExtractConnectFieldSchemaTest {

    private static final String EXTRACTED_FIELD_NAME = "extractedField";
    private ExtractConnectField<SinkRecord> transform = new ExtractConnectField<>();

    @After
    public void teardown() {
        transform.close();
    }

    @Before
    public void setup() {
        final Map<String, String> fields = new HashMap<>();
        fields.put("field", EXTRACTED_FIELD_NAME);
        fields.put("topics", "myLittleTopic");

        transform = new ExtractConnectField<>();
        transform.configure(fields);
    }

    @Test
    public void createRecordWithSchema() {
        final Schema innerSchema = buildInnerSchema();
        final Schema valueSchema = buildEnvelopeSchema(innerSchema);
        final Struct afterValue = buildInnerValue(innerSchema, "new payload");
        final Struct inputValue = buildInputValue(
                valueSchema,
                null,
                afterValue
        );
        final SinkRecord record = new SinkRecord(
                "myLittleTopic",
                0,
                null,
                null,
                valueSchema,
                inputValue,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        final Schema expectedSchema = buildTransformedSchema(valueSchema);
        final Struct expectedValue = buildTransformedValue(
                expectedSchema,
                null,
                inputValue.getStruct("after"),
                "new payload"
        );

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertEquals(expectedSchema, transformedRecord.valueSchema());
        assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void changeRecordWithSchema() {
        final Schema innerSchema = buildInnerSchema();
        final Schema valueSchema = buildEnvelopeSchema(innerSchema);
        final Struct inputValue = buildInputValue(
                valueSchema,
                buildInnerValue(innerSchema, "old payload"),
                buildInnerValue(innerSchema, "new payload")
        );
        final SinkRecord record = new SinkRecord(
                "myLittleTopic",
                0,
                null,
                null,
                valueSchema,
                inputValue,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        final Schema expectedSchema = buildTransformedSchema(valueSchema);
        final Struct expectedValue = buildTransformedValue(
                expectedSchema,
                inputValue.getStruct("before"),
                inputValue.getStruct("after"),
                "new payload"
        );

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertEquals(expectedSchema, transformedRecord.valueSchema());
        assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void deleteRecordWithSchema() {
        final Schema innerSchema = buildInnerSchema();
        final Schema valueSchema = buildEnvelopeSchema(innerSchema);
        final Struct inputValue = buildInputValue(
                valueSchema,
                buildInnerValue(innerSchema, "old payload"),
                null
        );
        final SinkRecord record = new SinkRecord(
                "myLittleTopic",
                0,
                null,
                null,
                valueSchema,
                inputValue,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        final Schema expectedSchema = buildTransformedSchema(valueSchema);
        final Struct expectedValue = buildTransformedValue(
                expectedSchema,
                inputValue.getStruct("before"),
                null,
                "old payload"
        );

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertEquals(expectedSchema, transformedRecord.valueSchema());
        assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void recordsWithoutSpecifiedTopicWillNotBeTransformed() {
        final Schema innerSchema = buildInnerSchema();
        final Schema valueSchema = buildEnvelopeSchema(innerSchema);
        final Struct inputValue = buildInputValue(
                valueSchema,
                buildInnerValue(innerSchema, "old payload"),
                buildInnerValue(innerSchema, "new payload")
        );
        final SinkRecord record = new SinkRecord(
                "unrelatedTopic",
                0,
                null,
                null,
                valueSchema,
                inputValue,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertEquals(valueSchema, transformedRecord.valueSchema());
        assertEquals(inputValue, transformedRecord.value());
    }

    @Test
    public void tombstoneRecordsWillNotBeTransformed() {
        final Schema valueSchema = buildEnvelopeSchema(buildInnerSchema());
        final SinkRecord record = new SinkRecord(
                "myLittleTopic",
                0,
                null,
                null,
                valueSchema,
                null,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertEquals(valueSchema, transformedRecord.valueSchema());
        assertNull(transformedRecord.value());
    }

    private Schema buildInnerSchema() {
        return SchemaBuilder.struct()
                .field(EXTRACTED_FIELD_NAME, Schema.STRING_SCHEMA)
                .optional()
                .build();
    }

    private static Schema buildEnvelopeSchema(final Schema innerSchema) {
        return SchemaBuilder.struct()
                .field("before", innerSchema)
                .field("after", innerSchema)
                .build();
    }

    private static Schema buildTransformedSchema(final Schema inputSchema) {
        return SchemaBuilder.struct()
                .field("before", inputSchema.field("before").schema())
                .field("after", inputSchema.field("after").schema())
                .field(EXTRACTED_FIELD_NAME, Schema.STRING_SCHEMA)
                .build();
    }

    private static Struct buildInputValue(
            final Schema inputMessageSchema,
            final Struct valueBefore,
            final Struct valueAfter
    ) {
        final Struct connectValue = new Struct(inputMessageSchema);
        connectValue.put("before", valueBefore);
        connectValue.put("after", valueAfter);

        return connectValue;
    }

    private static Struct buildInnerValue(final Schema schema, final String payload) {
        final Struct valueBefore = new Struct(schema);
        valueBefore.put(EXTRACTED_FIELD_NAME, payload);
        return valueBefore;
    }

    private Struct buildTransformedValue(
            final Schema schema,
            final Struct beforeValue,
            final Struct afterValue,
            final String extractedFieldValue
    ) {
        final Struct expectedValue = new Struct(schema);
        expectedValue.put("before", beforeValue);
        expectedValue.put("after", afterValue);
        expectedValue.put(EXTRACTED_FIELD_NAME, extractedFieldValue);
        return expectedValue;
    }
}

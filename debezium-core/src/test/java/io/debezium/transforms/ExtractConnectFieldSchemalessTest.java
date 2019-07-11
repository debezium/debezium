/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExtractConnectFieldSchemalessTest {

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
    public void createRecordWithoutSchema() {
        final Map<String, Object> after = buildInnerValue("new payload");
        final Map<String, Object> currentValue = buildInputValue(null, after);
        final SinkRecord record = new SinkRecord(
                "myLittleTopic",
                0, null,
                null,
                null,
                currentValue,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        final HashMap<String, Object> expectedValue = buildExpectedValue(
                null,
                after,
                "new payload"
        );
        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertNull(transformedRecord.valueSchema());
        assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void changeRecordWithoutSchema() {
        final Map<String, Object> before = buildInnerValue("old payload");
        final Map<String, Object> after = buildInnerValue("new payload");
        final Map<String, Object> currentValue = buildInputValue(before, after);
        final SinkRecord record = new SinkRecord(
                "myLittleTopic",
                0, null,
                null,
                null,
                currentValue,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        final HashMap<String, Object> expectedValue = buildExpectedValue(before, after, "new payload");
        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertNull(transformedRecord.valueSchema());
        assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void deleteRecordWithoutSchema() {
        final Map<String, Object> before = buildInnerValue("old payload");
        final Map<String, Object> currentValue = buildInputValue(before, null);
        final SinkRecord record = new SinkRecord(
                "myLittleTopic",
                0, null,
                null,
                null,
                currentValue,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        final HashMap<String, Object> expectedValue = buildExpectedValue(
                before,
                null,
                "old payload"
        );
        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertNull(transformedRecord.valueSchema());
        assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void recordsWithoutSpecifiedTopicWillNotBeTransformed() {
        final Map<String, Object> before = buildInnerValue("old payload");
        final Map<String, Object> after = buildInnerValue("new payload");
        final Map<String, Object> inputValue = buildInputValue(before, after);
        final SinkRecord record = new SinkRecord(
                "unrelatedTopic",
                0,
                null,
                null,
                null,
                inputValue,
                0
        );

        final SinkRecord transformedRecord = transform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.key());
        assertNull(transformedRecord.valueSchema());
        assertEquals(inputValue, transformedRecord.value());
    }

    private static Map<String, Object> buildInnerValue(final String payload) {
        return Collections.singletonMap(EXTRACTED_FIELD_NAME, payload);
    }

    private static Map<String, Object> buildInputValue(
            final Object before,
            final Object after
    ) {
        final Map<String, Object> result = new HashMap<>();
        result.put("before", before);
        result.put("after", after);
        return result;
    }

    private static HashMap<String, Object> buildExpectedValue(
            final Map<String, Object> beforeValue,
            final Map<String, Object> afterValue,
            final String extractedFieldValue
    ) {
        final HashMap<String, Object> expectedValue = new HashMap<>();
        expectedValue.put("before", beforeValue);
        expectedValue.put("after", afterValue);
        expectedValue.put(EXTRACTED_FIELD_NAME, extractedFieldValue);
        return expectedValue;
    }
}

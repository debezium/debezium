/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.assertj.core.api.Assertions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Testing;

/**
 * Test utility for checking {@link SourceRecord}.
 *
 * @author Randall Hauch
 */
public class VerifyRecord {

    @FunctionalInterface
    public interface RecordValueComparator {
        /**
         * Assert that the actual and expected values are equal. By the time this method is called, the actual value
         * and expected values are both determined to be non-null.
         *
         * @param pathToField the path to the field within the JSON representation of the source record; never null
         * @param actualValue the actual value for the field in the source record; never null
         * @param expectedValue the expected value for the field in the source record; never null
         */
        void assertEquals(String pathToField, Object actualValue, Object expectedValue);
    }

    private static final String APICURIO_URL = "http://localhost:8080/apis/registry/v2";

    private static final JsonConverter keyJsonConverter = new JsonConverter();
    private static final JsonConverter valueJsonConverter = new JsonConverter();
    private static final JsonDeserializer keyJsonDeserializer = new JsonDeserializer();
    private static final JsonDeserializer valueJsonDeserializer = new JsonDeserializer();

    private static final boolean useApicurio = isApucurioAvailable();
    private static Converter avroKeyConverter;
    private static Converter avroValueConverter;

    static {
        if (useApicurio) {
            avroKeyConverter = new io.apicurio.registry.utils.converter.AvroConverter();
            avroValueConverter = new io.apicurio.registry.utils.converter.AvroConverter();
        }
        else {
            MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
            avroKeyConverter = new AvroConverter(schemaRegistry);
            avroValueConverter = new AvroConverter(schemaRegistry);
        }

        Map<String, Object> config = new HashMap<>();
        config.put("schemas.enable", Boolean.TRUE.toString());
        config.put("schemas.cache.size", String.valueOf(100));
        keyJsonConverter.configure(config, true);
        keyJsonDeserializer.configure(config, true);
        valueJsonConverter.configure(config, false);
        valueJsonDeserializer.configure(config, false);

        config = new HashMap<>();
        if (useApicurio) {
            config.put("apicurio.registry.url", APICURIO_URL);
            config.put("apicurio.registry.auto-register", true);
            config.put("apicurio.registry.find-latest", true);
            config.put("apicurio.registry.check-period-ms", 1000);
        }
        else {
            config.put("schema.registry.url", "http://fake-url");
        }

        avroKeyConverter.configure(config, false);
        avroValueConverter.configure(config, false);
    }

    /**
     *
     */
    public VerifyRecord() {
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#CREATE INSERT/CREATE} record.
     *
     * @param record the source record; may not be null
     */
    public static void isValidInsert(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            assertThat(record.key()).isNotNull();
            assertThat(record.keySchema()).isNotNull();
        }
        else {
            assertThat(record.key()).isNull();
            assertThat(record.keySchema()).isNull();
        }

        assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(value.get(FieldName.AFTER)).isNotNull();
        assertThat(value.get(FieldName.BEFORE)).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#READ READ} record.
     *
     * @param record the source record; may not be null
     */
    public static void isValidRead(SourceRecord record) {
        assertThat(record.key()).isNotNull();
        assertThat(record.keySchema()).isNotNull();
        assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.READ.code());
        assertThat(value.get(FieldName.AFTER)).isNotNull();
        assertThat(value.get(FieldName.BEFORE)).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#UPDATE UPDATE} record.
     *
     * @param record the source record; may not be null
     */
    public static void isValidUpdate(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            assertThat(record.key()).isNotNull();
            assertThat(record.keySchema()).isNotNull();
        }
        else {
            assertThat(record.key()).isNull();
            assertThat(record.keySchema()).isNull();
        }
        assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.UPDATE.code());
        assertThat(value.get(FieldName.AFTER)).isNotNull();
        // assertThat(value.get(FieldName.BEFORE)).isNull(); // may be null
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#DELETE DELETE} record.
     *
     * @param record the source record; may not be null
     */
    public static void isValidDelete(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            assertThat(record.key()).isNotNull();
            assertThat(record.keySchema()).isNotNull();
        }
        else {
            assertThat(record.key()).isNull();
            assertThat(record.keySchema()).isNull();
        }
        assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.DELETE.code());
        assertThat(value.get(FieldName.BEFORE)).isNotNull();
        assertThat(value.get(FieldName.AFTER)).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a valid tombstone, meaning it has a non-null key and key schema but null
     * value and value schema.
     *
     * @param record the source record; may not be null
     */
    public static void isValidTombstone(SourceRecord record) {
        assertThat(record.key()).isNotNull();
        assertThat(record.keySchema()).isNotNull();
        assertThat(record.value()).isNull();
        assertThat(record.valueSchema()).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} has a valid non-null integer key that matches the expected integer value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void hasValidKey(SourceRecord record, String pkField, int pk) {
        Struct key = (Struct) record.key();
        assertThat(key.get(pkField)).isEqualTo(pk);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#CREATE INSERT/CREATE} record without primary key.
     *
     * @param record the source record; may not be null
     */
    public static void isValidInsert(SourceRecord record) {
        isValidInsert(record, false);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#CREATE INSERT/CREATE} record, and that the integer key
     * matches the expected value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void isValidInsert(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidInsert(record, true);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#CREATE READ} record, and that the integer key
     * matches the expected value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void isValidRead(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidRead(record);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#UPDATE UPDATE} record without PK.
     *
     * @param record the source record; may not be null
     */
    public static void isValidUpdate(SourceRecord record) {
        isValidUpdate(record, false);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#UPDATE UPDATE} record, and that the integer key
     * matches the expected value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void isValidUpdate(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidUpdate(record, true);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#DELETE DELETE} record without PK.
     * matches the expected value.
     *
     * @param record the source record; may not be null
     */
    public static void isValidDelete(SourceRecord record) {
        isValidDelete(record, false);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#DELETE DELETE} record, and that the integer key
     * matches the expected value.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void isValidDelete(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidDelete(record, true);
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#TRUNCATE TRUNCATE} record.
     *
     * @param record the source record; may not be null
     */
    public static void isValidTruncate(SourceRecord record) {
        assertThat(record.key()).isNull();

        assertThat(record.keySchema()).isNull();
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.TRUNCATE.code());
        assertThat(value.get(FieldName.BEFORE)).isNull();
        assertThat(value.get(FieldName.AFTER)).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a valid tombstone, meaning it has a valid non-null key with key schema
     * but null value and value schema.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void isValidTombstone(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidTombstone(record);
    }

    /**
     * Verify that the given {@link SourceRecord} has the appropriate source query value.
     *
     * @param record the source record; may not be null
     * @param query the expected sql query value.
     */
    public static void hasValidSourceQuery(final SourceRecord record, final String query) {
        assertValueField(record, "source/query", query);
    }

    /**
     * Verify that the given {@link SourceRecord} has no source query value.
     *
     * @param record the source record; may not be null
     */
    public static void hasNoSourceQuery(final SourceRecord record) {
        // Abstracted out so if we change from having a null value in the query field to
        // not including the query field entirely, we can easily update the assertion in one place.
        hasValidSourceQuery(record, null);
    }

    /**
     * Verify the given {@link SourceRecord} has the expected value in the given fieldPath.
     *
     * @param record the source record; may not be null
     * @param fieldPath the field path to validate, separated by '/'
     * @param expectedValue the expected value in the source records field path.
     */
    public static void assertValueField(SourceRecord record, String fieldPath, Object expectedValue) {
        Object value = record.value();
        String[] fieldNames = fieldPath.split("/");
        String pathSoFar = null;
        for (int i = 0; i != fieldNames.length; ++i) {
            String fieldName = fieldNames[i];
            if (value instanceof Struct) {
                value = ((Struct) value).get(fieldName);
            }
            else {
                // We expected the value to be a struct ...
                String path = pathSoFar == null ? "record value" : ("'" + pathSoFar + "'");
                String msg = "Expected the " + path + " to be a Struct but was " + value.getClass().getSimpleName() + " in record: " + SchemaUtil.asString(record);
                fail(msg);
            }
            pathSoFar = pathSoFar == null ? fieldName : pathSoFar + "/" + fieldName;
        }
        assertSameValue(value, expectedValue);
    }

    /**
     * Utility method to validate that two given {@link SourceRecord} values are identical.
     * @param actual actual value stored on the source record
     * @param expected expected value stored on the source record
     */
    public static void assertSameValue(Object actual, Object expected) {
        if (expected instanceof Double || expected instanceof Float || expected instanceof BigDecimal) {
            // Value should be within 1%
            double expectedNumericValue = ((Number) expected).doubleValue();
            double actualNumericValue = ((Number) actual).doubleValue();
            assertThat(actualNumericValue).isEqualTo(expectedNumericValue, Assertions.offset(0.01d * expectedNumericValue));
        }
        else if (expected instanceof Integer || expected instanceof Long || expected instanceof Short) {
            long expectedNumericValue = ((Number) expected).longValue();
            long actualNumericValue = ((Number) actual).longValue();
            assertThat(actualNumericValue).isEqualTo(expectedNumericValue);
        }
        else if (expected instanceof Boolean) {
            boolean expectedValue = (Boolean) expected;
            boolean actualValue = (Boolean) actual;
            assertThat(actualValue).isEqualTo(expectedValue);
        }
        else {
            assertThat(actual).isEqualTo(expected);
        }
    }

    /**
     * Assert that the supplied {@link Struct} is {@link Struct#validate() valid} and its {@link Struct#schema() schema}
     * matches that of the supplied {@code schema}.
     *
     * @param schemaAndValue the value with a schema; may not be null
     */
    public static void schemaMatchesStruct(SchemaAndValue schemaAndValue) {
        Object value = schemaAndValue.value();
        if (value == null) {
            // The schema should also be null ...
            assertThat(schemaAndValue.schema()).isNull();
        }
        else {
            // Both value and schema should exist and be valid ...
            assertThat(value).isInstanceOf(Struct.class);
            fieldsInSchema((Struct) value, schemaAndValue.schema());
        }
    }

    /**
     * Assert that the supplied {@link Struct} is {@link Struct#validate() valid} and its {@link Struct#schema() schema}
     * matches that of the supplied {@code schema}.
     *
     * @param struct the {@link Struct} to validate; may not be null
     * @param schema the expected schema of the {@link Struct}; may not be null
     */
    public static void schemaMatchesStruct(Struct struct, Schema schema) {
        // First validate the struct itself ...
        try {
            struct.validate();
        }
        catch (DataException e) {
            throw new AssertionError("The struct '" + struct + "' failed to validate", e);
        }

        Schema actualSchema = struct.schema();
        assertThat(actualSchema).isEqualTo(schema);
        fieldsInSchema(struct, schema);
    }

    /**
     * Verify that the fields in the given {@link Struct} reference the {@link Field} definitions in the given {@link Schema}.
     *
     * @param struct the {@link Struct} instance; may not be null
     * @param schema the {@link Schema} defining the fields in the Struct; may not be null
     */
    public static void fieldsInSchema(Struct struct, Schema schema) {
        schema.fields().forEach(field -> {
            Object val1 = struct.get(field);
            Object val2 = struct.get(field.name());
            assertThat(val1).isSameAs(val2);
            if (val1 instanceof Struct) {
                fieldsInSchema((Struct) val1, field.schema());
            }
        });
    }

    /**
     * Print a message with the JSON representation of the SourceRecord.
     *
     * @param record the source record; may not be null
     */
    public static void print(SourceRecord record) {
        Testing.print(SchemaUtil.asString(record));
    }

    /**
     * Print a debug message with the JSON representation of the SourceRecord.
     *
     * @param record the source record; may not be null
     */
    public static void debug(SourceRecord record) {
        Testing.debug(SchemaUtil.asDetailedString(record));
    }

    public static void assertEquals(SourceRecord actual, SourceRecord expected, Predicate<String> ignoreFields,
                                    Map<String, RecordValueComparator> comparatorsByName,
                                    Map<String, RecordValueComparator> comparatorsBySchemaName) {
        assertThat(actual).isNotNull();
        assertThat(expected).isNotNull();
        assertEquals(null, actual.sourcePartition(), expected.sourcePartition(), "sourcePartition", "", ignoreFields, comparatorsByName, comparatorsBySchemaName);
        assertEquals(null, actual.sourceOffset(), expected.sourceOffset(), "sourceOffset", "", ignoreFields, comparatorsByName, comparatorsBySchemaName);
        assertThat(actual.topic()).isEqualTo(expected.topic());
        assertThat(actual.kafkaPartition()).isEqualTo(expected.kafkaPartition());
        Schema actualKeySchema = actual.keySchema();
        Schema actualValueSchema = actual.valueSchema();
        Schema expectedKeySchema = expected.keySchema();
        Schema expectedValueSchema = expected.valueSchema();
        if (!Objects.equals(actualKeySchema, expectedKeySchema)) {
            String actualStr = SchemaUtil.asString(actualKeySchema);
            String expectedStr = SchemaUtil.asString(expectedKeySchema);
            assertThat(actualStr).as("The key schema for record with key " + SchemaUtil.asString(actual.key())
                    + " did not match expected schema").isEqualTo(expectedStr);
        }
        if (!Objects.equals(actualValueSchema, expectedValueSchema)) {
            String actualStr = SchemaUtil.asString(actualValueSchema);
            String expectedStr = SchemaUtil.asString(expectedValueSchema);
            assertThat(actualStr).isEqualTo(expectedStr);
        }
        assertEquals(actualKeySchema, actual.key(), expected.key(), "key", "", ignoreFields, comparatorsByName, comparatorsBySchemaName);
        assertEquals(actualValueSchema, actual.value(), expected.value(), "value", "", ignoreFields, comparatorsByName, comparatorsBySchemaName);
    }

    /**
     * Asserts that the two given schemas are equal.
     *
     * @param fieldName
     *            name of the field owning that schema, if it's not a top-level schema
     * @param actual
     *            the actual schema
     * @param expected
     *            the expected schema
     */
    public static void assertConnectSchemasAreEqual(String fieldName, Schema actual, Schema expected) {
        if (!areConnectSchemasEqual(actual, expected)) {
            // first try failing with an assertion message that shows the actual difference
            assertThat(SchemaUtil.asString(actual)).describedAs("field name: " + fieldName).isEqualTo(SchemaUtil.asString(expected));

            // compare schema parameters
            assertThat(actual.parameters()).describedAs("field '" + fieldName + "' parameters").isEqualTo(expected.parameters());

            // fall-back just in case (e.g. differences of element schemas of arrays)
            fail("field '" + fieldName + "': " + SchemaUtil.asString(actual) + " was not equal to " + SchemaUtil.asString(expected));
        }
    }

    protected static String nameOf(String keyOrValue, String field) {
        if (field == null || field.trim().isEmpty()) {
            return keyOrValue;
        }
        return "'" + field + "' field in the record " + keyOrValue;
    }

    private static String fieldName(String field, String suffix) {
        if (field == null || field.trim().isEmpty()) {
            return suffix;
        }
        return field + "/" + suffix;
    }

    private static String schemaName(Schema schema) {
        if (schema == null) {
            return null;
        }
        String name = schema.name();
        if (name != null) {
            name = name.trim();
        }
        return name == null || name.isEmpty() ? null : name;
    }

    @SuppressWarnings("unchecked")
    protected static void assertEquals(Schema schema, Object o1, Object o2, String keyOrValue, String field,
                                       Predicate<String> ignoreFields,
                                       Map<String, RecordValueComparator> comparatorsByName,
                                       Map<String, RecordValueComparator> comparatorsBySchemaName) {
        if (o1 == o2) {
            return;
        }
        if (o1 == null) {
            if (o2 == null) {
                return;
            }
            fail(nameOf(keyOrValue, field) + " was null but expected " + SchemaUtil.asString(o2));
        }
        else if (o2 == null) {
            fail("expecting a null " + nameOf(keyOrValue, field) + " but found " + SchemaUtil.asString(o1));
        }
        // See if there is a custom comparator for this field ...
        String pathToField = keyOrValue.toUpperCase() + "/" + field;
        RecordValueComparator comparator = comparatorsByName.get(pathToField);
        if (comparator != null) {
            comparator.assertEquals(nameOf(keyOrValue, field), o1, o2);
            return;
        }
        // See if there is a custom comparator for this schema type ...
        String schemaName = schemaName(schema);
        if (schemaName != null) {
            comparator = comparatorsBySchemaName.get(schemaName);
            if (comparator != null) {
                comparator.assertEquals(nameOf(keyOrValue, field), o1, o2);
            }
        }

        if (o1 instanceof ByteBuffer) {
            o1 = ((ByteBuffer) o1).array();
        }
        if (o2 instanceof ByteBuffer) {
            o2 = ((ByteBuffer) o2).array();
        }
        if (o2 instanceof byte[]) {
            if (!(o1 instanceof byte[])) {
                fail("expecting " + nameOf(keyOrValue, field) + " to be byte[] but found " + o1.getClass().toString());
            }
            if (!Arrays.equals((byte[]) o1, (byte[]) o2)) {
                fail("byte[] at " + nameOf(keyOrValue, field) + " is " + o1 + " but was expected to be " + o2);
            }
        }
        else if (o2 instanceof Object[]) {
            if (!(o1 instanceof Object[])) {
                fail("expecting " + nameOf(keyOrValue, field) + " to be Object[] but was " + o1.getClass().toString());
            }
            if (!deepEquals((Object[]) o1, (Object[]) o2)) {
                fail("Object[] at " + nameOf(keyOrValue, field) + " is " + o1 + " but was expected to be " + o2);
            }
        }
        else if (o2 instanceof Map) {
            if (!(o1 instanceof Map)) {
                fail("expecting " + nameOf(keyOrValue, field) + " to be Map<String,?> but was " + o1.getClass().toString());
            }
            Map<String, Object> m1 = (Map<String, Object>) o1;
            Map<String, Object> m2 = (Map<String, Object>) o2;
            if (!m1.keySet().equals(m2.keySet())) {
                fail("Map at " + nameOf(keyOrValue, field) + " has entry keys " + m1.keySet() + " but expected " + m2.keySet());
            }
            for (Map.Entry<String, Object> entry : m1.entrySet()) {
                String key = entry.getKey();
                String fieldName = field.isEmpty() ? key : field + "/" + key;
                String predicate = keyOrValue.toUpperCase() + "/" + fieldName;
                if (ignoreFields != null && ignoreFields.test(predicate)) {
                    continue;
                }
                Object v1 = entry.getValue();
                Object v2 = m2.get(key);
                assertEquals(null, v1, v2, keyOrValue, fieldName(field, key), ignoreFields,
                        comparatorsByName, comparatorsBySchemaName);
            }
        }
        else if (o2 instanceof Collection) {
            if (!(o1 instanceof Collection)) {
                fail("expecting " + nameOf(keyOrValue, field) + " to be Collection<?> but was " + o1.getClass().toString());
            }
            Collection<Object> m1 = (Collection<Object>) o1;
            Collection<Object> m2 = (Collection<Object>) o2;
            if (m1.size() != m2.size()) {
                fail("Collection at " + nameOf(keyOrValue, field) + " has " + SchemaUtil.asString(m1) + " but expected "
                        + SchemaUtil.asString(m2));
            }
            Iterator<?> iter1 = m1.iterator();
            Iterator<?> iter2 = m2.iterator();
            int index = 0;
            while (iter1.hasNext() && iter2.hasNext()) {
                assertEquals(null, iter1.next(), iter2.next(), keyOrValue, field + "[" + (index++) + "]", ignoreFields,
                        comparatorsByName, comparatorsBySchemaName);
            }
        }
        else if (o2 instanceof Struct) {
            if (!(o1 instanceof Struct)) {
                fail("expecting " + nameOf(keyOrValue, field) + " to be Struct but was " + o1.getClass().toString());
            }
            // Unfortunately, the Struct.equals() method has a bug in that it is not using Arrays.deepEquals(...) to
            // compare values in two Struct objects. The result is that the equals only works if the values of the
            // first level Struct are non arrays; otherwise, the array values are compared using == and that obviously
            // does not work for non-primitive values.
            Struct struct1 = (Struct) o1;
            Struct struct2 = (Struct) o2;
            if (!Objects.equals(struct1.schema(), struct2.schema())) {
                fail("Schema at " + nameOf(keyOrValue, field) + " is " + SchemaUtil.asString(struct1.schema()) + " but expected "
                        + SchemaUtil.asString(struct2.schema()));
            }
            for (Field f : struct1.schema().fields()) {
                String fieldName = fieldName(field, f.name());
                String predicate = keyOrValue.toUpperCase() + "/" + fieldName;
                if (ignoreFields != null && ignoreFields.test(predicate)) {
                    continue;
                }
                Object value1 = struct1.get(f);
                Object value2 = struct2.get(f);
                assertEquals(f.schema(), value1, value2, keyOrValue, fieldName, ignoreFields,
                        comparatorsByName, comparatorsBySchemaName);
            }
            return;
        }
        else if (o2 instanceof Double || o2 instanceof Float || o2 instanceof BigDecimal) {
            // Value should be within 1%
            double expectedNumericValue = ((Number) o2).doubleValue();
            double actualNumericValue = ((Number) o1).doubleValue();
            String desc = "found " + nameOf(keyOrValue, field) + " is " + o1 + " but expected " + o2;
            assertThat(actualNumericValue).as(desc).isEqualTo(expectedNumericValue, Assertions.offset(0.01d * expectedNumericValue));
        }
        else if (o2 instanceof Integer || o2 instanceof Long || o2 instanceof Short) {
            long expectedNumericValue = ((Number) o2).longValue();
            long actualNumericValue = ((Number) o1).longValue();
            String desc = "found " + nameOf(keyOrValue, field) + " is " + o1 + " but expected " + o2;
            assertThat(actualNumericValue).as(desc).isEqualTo(expectedNumericValue);
        }
        else if (o2 instanceof Boolean) {
            boolean expectedValue = ((Boolean) o2).booleanValue();
            boolean actualValue = ((Boolean) o1).booleanValue();
            String desc = "found " + nameOf(keyOrValue, field) + " is " + o1 + " but expected " + o2;
            assertThat(actualValue).as(desc).isEqualTo(expectedValue);
        }
        else if (ZonedTimestamp.SCHEMA_NAME.equals(schemaName)) {
            // the actual value (produced by the connectors) should always be properly formatted
            String actualValueString = o1.toString();
            ZonedDateTime actualValue = ZonedDateTime.parse(o1.toString(), ZonedTimestamp.FORMATTER);

            String expectedValueString = o2.toString();
            ZonedDateTime expectedValue;
            try {
                // first try a standard offset format which contains the TZ information
                expectedValue = ZonedDateTime.parse(expectedValueString, ZonedTimestamp.FORMATTER);
            }
            catch (DateTimeParseException e) {
                // then try a local format using the system default offset
                LocalDateTime localDateTime = LocalDateTime.parse(expectedValueString);
                expectedValue = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
            }
            assertThat(actualValue.toInstant()).as(actualValueString).isEqualTo(expectedValue.toInstant()).as(expectedValueString);
        }
        else {
            assertThat(o1).isEqualTo(o2);
        }
    }

    /**
     * Validate that a {@link SourceRecord}'s key and value can each be converted to a byte[] and then back to an equivalent
     * {@link SourceRecord}.
     *
     * @param record the record to validate; may not be null
     */
    public static void isValid(SourceRecord record) {
        isValid(record, false);
    }

    /**
     * Validate that a {@link SourceRecord}'s key and value can each be converted to a byte[] and then back to an equivalent
     * {@link SourceRecord}.
     *
     * @param record the record to validate; may not be null
     * @param ignoreAvro true when Avro check should not be executed
     */
    public static void isValid(SourceRecord record, boolean ignoreAvro) {
        // print(record);

        JsonNode keyJson = null;
        JsonNode valueJson = null;
        SchemaAndValue keyWithSchema = null;
        SchemaAndValue valueWithSchema = null;
        SchemaAndValue avroKeyWithSchema = null;
        SchemaAndValue avroValueWithSchema = null;
        String msg = null;
        try {
            // The key can be null for tables that do not have a primary or unique key ...
            if (record.key() != null) {
                msg = "checking key is not null";
                assertThat(record.key()).isNotNull();
                assertThat(record.keySchema()).isNotNull();
            }
            else {
                msg = "checking key schema and key are both null";
                assertThat(record.key()).isNull();
                assertThat(record.keySchema()).isNull();
            }

            // If the value is not null there must be a schema; otherwise, the schema should also be null ...
            if (record.value() == null) {
                msg = "checking value schema is null";
                assertThat(record.valueSchema()).isNull();
                msg = "checking key is not null when value is null";
                assertThat(record.key()).isNotNull();
            }
            else {
                msg = "checking value schema is not null";
                assertThat(record.valueSchema()).isNotNull();
            }

            // First serialize and deserialize the key ...
            msg = "serializing key using JSON converter";
            byte[] keyBytes = keyJsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            msg = "deserializing key using JSON deserializer";
            keyJson = keyJsonDeserializer.deserialize(record.topic(), keyBytes);
            msg = "deserializing key using JSON converter";
            keyWithSchema = keyJsonConverter.toConnectData(record.topic(), keyBytes);
            msg = "comparing key schema to that serialized/deserialized with JSON converter";
            assertThat(keyWithSchema.schema()).isEqualTo(record.keySchema());
            msg = "comparing key to that serialized/deserialized with JSON converter";
            assertThat(keyWithSchema.value()).isEqualTo(record.key());
            msg = "comparing key to its schema";
            schemaMatchesStruct(keyWithSchema);

            // then the value ...
            msg = "serializing value using JSON converter";
            byte[] valueBytes = valueJsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using JSON deserializer";
            valueJson = valueJsonDeserializer.deserialize(record.topic(), valueBytes);
            msg = "deserializing value using JSON converter";
            valueWithSchema = valueJsonConverter.toConnectData(record.topic(), valueBytes);
            msg = "comparing value schema to that serialized/deserialized with JSON converter";
            assertEquals(valueWithSchema.schema(), record.valueSchema());
            msg = "comparing value to that serialized/deserialized with JSON converter";
            assertEquals(valueWithSchema.value(), record.value());
            msg = "comparing value to its schema";
            schemaMatchesStruct(valueWithSchema);

            // Introduced due to https://github.com/confluentinc/schema-registry/issues/1693
            // and https://issues.redhat.com/browse/DBZ-3535
            if (ignoreAvro) {
                return;
            }

            // Make sure the schema names are valid Avro schema names ...
            validateSchemaNames(record.keySchema());
            validateSchemaNames(record.valueSchema());

            // Serialize and deserialize the key using the Avro converter, and check that we got the same result ...
            msg = "serializing key using Avro converter";
            byte[] avroKeyBytes = avroValueConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            msg = "deserializing key using Avro converter";
            avroKeyWithSchema = avroValueConverter.toConnectData(record.topic(), avroKeyBytes);
            msg = "comparing key schema to that serialized/deserialized with Avro converter";
            assertEquals(setVersion(avroKeyWithSchema.schema(), null), setVersion(record.keySchema(), null));
            msg = "comparing key to that serialized/deserialized with Avro converter";
            assertEquals(setVersion(avroKeyWithSchema, null).value(), setVersion(record.key(), null));
            msg = "comparing key to its schema";
            schemaMatchesStruct(avroKeyWithSchema);

            // Serialize and deserialize the value using the Avro converter, and check that we got the same result ...
            msg = "serializing value using Avro converter";
            byte[] avroValueBytes = avroValueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using Avro converter";
            avroValueWithSchema = avroValueConverter.toConnectData(record.topic(), avroValueBytes);
            msg = "comparing value schema to that serialized/deserialized with Avro converter";
            assertEquals(setVersion(avroValueWithSchema.schema(), null), setVersion(record.valueSchema(), null));
            msg = "comparing value to that serialized/deserialized with Avro converter";
            assertEquals(setVersion(avroValueWithSchema, null).value(), setVersion(record.value(), null));
            msg = "comparing value to its schema";
            schemaMatchesStruct(avroValueWithSchema);
        }
        catch (Throwable t) {
            Testing.Print.enable();
            Testing.print("Problem with message on topic '" + record.topic() + "':");
            Testing.printError(t);
            Testing.print("error " + msg);
            Testing.print("  key: " + SchemaUtil.asString(record.key()));
            Testing.print("  key deserialized from JSON: " + prettyJson(keyJson));
            if (keyWithSchema != null) {
                Testing.print("  key to/from JSON: " + SchemaUtil.asString(keyWithSchema.value()));
            }
            if (avroKeyWithSchema != null) {
                Testing.print("  key to/from Avro: " + SchemaUtil.asString(avroKeyWithSchema.value()));
            }
            Testing.print("  value: " + SchemaUtil.asString(record.value()));
            Testing.print("  value deserialized from JSON: " + prettyJson(valueJson));
            if (valueWithSchema != null) {
                Testing.print("  value to/from JSON: " + SchemaUtil.asString(valueWithSchema.value()));
            }
            if (avroValueWithSchema != null) {
                Testing.print("  value to/from Avro: " + SchemaUtil.asString(avroValueWithSchema.value()));
            }
            if (t instanceof AssertionError) {
                throw t;
            }
            fail("error " + msg + ": " + t.getMessage());
        }
    }

    protected static void validateSchemaNames(Schema schema) {
        if (schema == null) {
            return;
        }
        String schemaName = schema.name();
        if (schemaName != null && !SchemaNameAdjuster.isValidFullname(schemaName)) {
            fail("Kafka schema '" + schemaName + "' is not a valid Avro schema name");
        }
        if (schema.type() == Type.STRUCT) {
            schema.fields().forEach(field -> {
                validateSubSchemaNames(schema, field);
            });
        }
    }

    protected static void validateSubSchemaNames(Schema parentSchema, Field field) {
        if (field == null) {
            return;
        }
        Schema subSchema = field.schema();
        String subSchemaName = subSchema.name();
        if (subSchemaName != null && !SchemaNameAdjuster.isValidFullname(subSchemaName)) {
            fail("Kafka schema '" + parentSchema.name() + "' contains a subschema for '" + field.name() + "' named '" + subSchema.name()
                    + "' that is not a valid Avro schema name");
        }
        if (subSchema.type() == Type.STRUCT) {
            subSchema.fields().forEach(child -> {
                validateSubSchemaNames(parentSchema, child);
            });
        }
    }

    protected static void printJson(SourceRecord record) {
        JsonNode keyJson = null;
        JsonNode valueJson = null;
        try {
            // First serialize and deserialize the key ...
            byte[] keyBytes = keyJsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            keyJson = keyJsonDeserializer.deserialize(record.topic(), keyBytes);
            // then the value ...
            byte[] valueBytes = valueJsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            valueJson = valueJsonDeserializer.deserialize(record.topic(), valueBytes);
            // And finally get ready to print it ...
            JsonNodeFactory nodeFactory = new JsonNodeFactory(false);
            ObjectNode message = nodeFactory.objectNode();
            message.set("key", keyJson);
            message.set("value", valueJson);
            Testing.print("Message on topic '" + record.topic() + "':");
            Testing.print(prettyJson(message));
        }
        catch (Throwable t) {
            Testing.printError(t);
            Testing.print("Problem with message on topic '" + record.topic() + "':");
            if (keyJson != null) {
                Testing.print("valid key = " + prettyJson(keyJson));
            }
            else {
                Testing.print("invalid key");
            }
            if (valueJson != null) {
                Testing.print("valid value = " + prettyJson(valueJson));
            }
            else {
                Testing.print("invalid value");
            }
            fail(t.getMessage());
        }
    }

    protected static String prettyJson(JsonNode json) {
        try {
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(json);
        }
        catch (Throwable t) {
            Testing.printError(t);
            fail(t.getMessage());
            assert false : "Will not get here";
            return null;
        }
    }

    // The remaining methods are needed simply because of the KAFKA-3803, so our comparisons cannot rely upon Struct.equals

    protected static void assertEquals(Object o1, Object o2) {
        // assertThat(o1).isEqualTo(o2);

        if (o1 instanceof Schema && o2 instanceof Schema) {
            assertConnectSchemasAreEqual(null, (Schema) o1, (Schema) o2);
        }
        else if (!equals(o1, o2)) {
            fail(SchemaUtil.asString(o1) + " was not equal to " + SchemaUtil.asString(o2));
        }
    }

    @SuppressWarnings("unchecked")
    protected static boolean equals(Object o1, Object o2) {
        if (o1 == o2) {
            return true;
        }
        if (o1 == null) {
            return o2 == null ? true : false;
        }
        if (o2 == null) {
            return false;
        }
        if (o1 instanceof ByteBuffer) {
            o1 = ((ByteBuffer) o1).array();
        }
        if (o2 instanceof ByteBuffer) {
            o2 = ((ByteBuffer) o2).array();
        }
        if (o1 instanceof byte[] && o2 instanceof byte[]) {
            boolean result = Arrays.equals((byte[]) o1, (byte[]) o2);
            return result;
        }
        if (o1 instanceof Object[] && o2 instanceof Object[]) {
            boolean result = deepEquals((Object[]) o1, (Object[]) o2);
            return result;
        }
        if (o1 instanceof Map && o2 instanceof Map) {
            Map<String, Object> m1 = (Map<String, Object>) o1;
            Map<String, Object> m2 = (Map<String, Object>) o2;
            if (!m1.keySet().equals(m2.keySet())) {
                return false;
            }

            for (Map.Entry<String, Object> entry : m1.entrySet()) {
                Object v1 = entry.getValue();
                Object v2 = m2.get(entry.getKey());
                if (!equals(v1, v2)) {
                    return false;
                }
            }
            return true;
        }
        if (o1 instanceof Collection && o2 instanceof Collection) {
            Collection<Object> m1 = (Collection<Object>) o1;
            Collection<Object> m2 = (Collection<Object>) o2;
            if (m1.size() != m2.size()) {
                return false;
            }
            Iterator<?> iter1 = m1.iterator();
            Iterator<?> iter2 = m2.iterator();
            while (iter1.hasNext() && iter2.hasNext()) {
                if (!equals(iter1.next(), iter2.next())) {
                    return false;
                }
            }
            return true;
        }
        if (o1 instanceof Struct && o2 instanceof Struct) {
            // Unfortunately, the Struct.equals() method has a bug in that it is not using Arrays.deepEquals(...) to
            // compare values in two Struct objects. The result is that the equals only works if the values of the
            // first level Struct are non arrays; otherwise, the array values are compared using == and that obviously
            // does not work for non-primitive values.
            Struct struct1 = (Struct) o1;
            Struct struct2 = (Struct) o2;
            if (!areConnectSchemasEqual(struct1.schema(), struct2.schema())) {
                return false;
            }
            Object[] array1 = valuesFor(struct1);
            Object[] array2 = valuesFor(struct2);

            return deepEquals(array1, array2);
        }

        if (o1 instanceof ConnectSchema && o1 instanceof ConnectSchema) {
            return areConnectSchemasEqual((ConnectSchema) o1, (ConnectSchema) o2);
        }

        return Objects.equals(o1, o2);
    }

    private static Object[] valuesFor(Struct struct) {
        Object[] array = new Object[struct.schema().fields().size()];
        int index = 0;
        for (Field field : struct.schema().fields()) {
            array[index] = struct.get(field);
            ++index;
        }
        return array;
    }

    private static boolean deepEquals(Object[] a1, Object[] a2) {
        if (a1 == a2) {
            return true;
        }
        if (a1 == null || a2 == null) {
            return false;
        }
        int length = a1.length;
        if (a2.length != length) {
            return false;
        }

        for (int i = 0; i < length; i++) {
            Object e1 = a1[i];
            Object e2 = a2[i];

            if (e1 == e2) {
                continue;
            }
            if (e1 == null) {
                return false;
            }

            // Figure out whether the two elements are equal
            boolean eq = deepEquals0(e1, e2);

            if (!eq) {
                return false;
            }
        }
        return true;
    }

    private static boolean deepEquals0(Object e1, Object e2) {
        assert e1 != null;
        boolean eq;
        if (e1 instanceof Object[] && e2 instanceof Object[]) {
            eq = deepEquals((Object[]) e1, (Object[]) e2);
        }
        else if (e1 instanceof byte[] && e2 instanceof byte[]) {
            eq = Arrays.equals((byte[]) e1, (byte[]) e2);
        }
        else if (e1 instanceof short[] && e2 instanceof short[]) {
            eq = Arrays.equals((short[]) e1, (short[]) e2);
        }
        else if (e1 instanceof int[] && e2 instanceof int[]) {
            eq = Arrays.equals((int[]) e1, (int[]) e2);
        }
        else if (e1 instanceof long[] && e2 instanceof long[]) {
            eq = Arrays.equals((long[]) e1, (long[]) e2);
        }
        else if (e1 instanceof char[] && e2 instanceof char[]) {
            eq = Arrays.equals((char[]) e1, (char[]) e2);
        }
        else if (e1 instanceof float[] && e2 instanceof float[]) {
            eq = Arrays.equals((float[]) e1, (float[]) e2);
        }
        else if (e1 instanceof double[] && e2 instanceof double[]) {
            eq = Arrays.equals((double[]) e1, (double[]) e2);
        }
        else if (e1 instanceof boolean[] && e2 instanceof boolean[]) {
            eq = Arrays.equals((boolean[]) e1, (boolean[]) e2);
        }
        else {
            eq = equals(e1, e2);
        }
        return eq;
    }

    private static boolean areConnectSchemasEqual(Schema schema1, Schema schema2) {
        if (schema1 == schema2) {
            return true;
        }
        if (schema1 == null && schema2 != null || schema1 != null && schema2 == null) {
            return false;
        }
        if (schema1.getClass() != schema2.getClass()) {
            return false;
        }

        boolean keySchemasEqual = true;
        boolean valueSchemasEqual = true;
        boolean fieldsEqual = true;
        if (schema1.type() == Type.MAP && schema2.type() == Type.MAP) {
            keySchemasEqual = Objects.equals(schema1.keySchema(), schema2.keySchema());
            valueSchemasEqual = Objects.equals(schema1.valueSchema(), schema2.valueSchema());
        }
        else if (schema1.type() == Type.ARRAY && schema2.type() == Type.ARRAY) {
            valueSchemasEqual = areConnectSchemasEqual(schema1.valueSchema(), schema2.valueSchema());
        }
        else if (schema1.type() == Type.STRUCT && schema2.type() == Type.STRUCT) {
            fieldsEqual = areFieldListsEqual(schema1.fields(), schema2.fields());
        }

        boolean equal = Objects.equals(schema1.isOptional(), schema2.isOptional()) &&
                Objects.equals(schema1.version(), schema2.version()) &&
                Objects.equals(schema1.name(), schema2.name()) &&
                Objects.equals(schema1.doc(), schema2.doc()) &&
                Objects.equals(schema1.type(), schema2.type()) &&
                fieldsEqual &&
                keySchemasEqual &&
                valueSchemasEqual &&
                Objects.equals(schema1.parameters(), schema2.parameters());

        // The default value after de-serialization can be of byte[] even if the value
        // before serialization is ByteBuffer. This must be taken into account for comparison.
        if (equal) {
            Object default1 = schema1.defaultValue();
            Object default2 = schema2.defaultValue();
            if (default1 instanceof ByteBuffer && default2 instanceof byte[]) {
                default1 = ((ByteBuffer) default1).array();
            }
            if (default1 instanceof byte[] && default2 instanceof ByteBuffer) {
                default2 = ((ByteBuffer) default2).array();
            }
            equal = Objects.deepEquals(default1, default2);
        }

        return equal;
    }

    private static boolean areFieldListsEqual(List<Field> fields1, List<Field> fields2) {
        if (fields1 == null && fields2 != null || fields1 != null && fields2 == null) {
            return false;
        }

        if (fields1.size() != fields2.size()) {
            return false;
        }

        for (int i = 0; i < fields1.size(); i++) {
            Field field1 = fields1.get(i);
            Field field2 = fields2.get(i);

            boolean equal = Objects.equals(field1.index(), field2.index()) &&
                    Objects.equals(field1.name(), field2.name()) &&
                    areConnectSchemasEqual(field1.schema(), field2.schema());

            if (!equal) {
                return false;
            }
        }

        return true;
    }

    /**
     * Sets the version of a passed schema to a new value.
     *
     * @param schema the schema to be updated
     * @param version the target version value
     * @return the new schema with the same structure but updated version
     */
    private static Schema setVersion(Schema schema, Integer version) {
        if (schema == null) {
            return null;
        }
        final SchemaBuilder builder = new SchemaBuilder(schema.type())
                .name(schema.name())
                .version(version)
                .doc(schema.doc());
        if (schema.defaultValue() != null) {
            builder.defaultValue(schema.defaultValue());
        }
        if (schema.isOptional()) {
            builder.optional();
        }
        if (schema.parameters() != null) {
            builder.parameters(schema.parameters());
        }
        if (schema.fields() != null) {
            for (Field f : schema.fields()) {
                builder.field(f.name(), f.schema());
            }
        }
        return builder.build();
    }

    /**
     * Sets the version of a passed schema to a new value.
     *
     * @param value the value with schema to be updated
     * @param version the target version value
     * @return the new value with the same schema but updated version
     */
    private static SchemaAndValue setVersion(SchemaAndValue value, Integer version) {
        final Schema schema = setVersion(value.schema(), version);
        if (schema == null) {
            return value;
        }
        if (schema.type() != Type.STRUCT) {
            return new SchemaAndValue(schema, value);
        }
        final Struct struct = new Struct(schema);
        final Struct old = (Struct) value.value();
        for (Field f : schema.fields()) {
            struct.put(f, old.getWithoutDefault(f.name()));
        }
        return new SchemaAndValue(schema, struct);
    }

    /**
     * Sets the version of a passed schema to a new value.
     *
     * @param obj the value with schema to be updated
     * @param version the target version value
     * @return the new value with the same schema but updated version
     */
    private static Object setVersion(Object obj, Integer version) {
        if (!(obj instanceof Struct)) {
            return obj;
        }
        final Struct value = (Struct) obj;
        final Schema schema = setVersion(value.schema(), version);
        if (schema == null) {
            return value;
        }
        if (schema.type() != Type.STRUCT) {
            return value;
        }
        final Struct struct = new Struct(schema);
        final Struct old = value;
        for (Field f : schema.fields()) {
            struct.put(f, old.getWithoutDefault(f.name()));
        }
        return struct;
    }

    public static boolean isApucurioAvailable() {
        String useApicurio = System.getProperty("use.apicurio");
        return useApicurio != null && useApicurio.equalsIgnoreCase("true");
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.fest.assertions.Assertions.assertThat;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Envelope.Operation;
import io.debezium.util.AvroValidator;
import io.debezium.util.Testing;

/**
 * Test utility for checking {@link SourceRecord}.
 * 
 * @author Randall Hauch
 */
public class VerifyRecord {

    private static final JsonConverter keyJsonConverter = new JsonConverter();
    private static final JsonConverter valueJsonConverter = new JsonConverter();
    private static final JsonDeserializer keyJsonDeserializer = new JsonDeserializer();
    private static final JsonDeserializer valueJsonDeserializer = new JsonDeserializer();

    private static final MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    private static final AvroConverter avroKeyConverter = new AvroConverter(schemaRegistry);
    private static final AvroConverter avroValueConverter = new AvroConverter(schemaRegistry);

    static {
        Map<String, Object> config = new HashMap<>();
        config.put("schemas.enable", Boolean.TRUE.toString());
        config.put("schemas.cache.size", 100);
        keyJsonConverter.configure(config, true);
        keyJsonDeserializer.configure(config, true);
        valueJsonConverter.configure(config, false);
        valueJsonDeserializer.configure(config, false);

        config = new HashMap<>();
        config.put("schema.registry.url", "http://fake-url");
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
    public static void isValidInsert(SourceRecord record) {
        assertThat(record.key()).isNotNull();
        assertThat(record.keySchema()).isNotNull();
        assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        assertThat(value).isNotNull();
        assertThat(value.getString(FieldName.OPERATION)).isEqualTo(Operation.CREATE.code());
        assertThat(value.get(FieldName.AFTER)).isNotNull();
        assertThat(value.get(FieldName.BEFORE)).isNull();
    }

    /**
     * Verify that the given {@link SourceRecord} is a {@link Operation#UPDATE UPDATE} record.
     * 
     * @param record the source record; may not be null
     */
    public static void isValidUpdate(SourceRecord record) {
        assertThat(record.key()).isNotNull();
        assertThat(record.keySchema()).isNotNull();
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
    public static void isValidDelete(SourceRecord record) {
        assertThat(record.key()).isNotNull();
        assertThat(record.keySchema()).isNotNull();
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
     * Verify that the given {@link SourceRecord} is a {@link Operation#CREATE INSERT/CREATE} record, and that the integer key
     * matches the expected value.
     * 
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     * @param pk the expected integer value of the primary key in the struct
     */
    public static void isValidInsert(SourceRecord record, String pkField, int pk) {
        hasValidKey(record, pkField, pk);
        isValidInsert(record);
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
        isValidUpdate(record);
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
        isValidDelete(record);
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
        } else {
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
        } catch (DataException e) {
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

    /**
     * Validate that a {@link SourceRecord}'s key and value can each be converted to a byte[] and then back to an equivalent
     * {@link SourceRecord}.
     * 
     * @param record the record to validate; may not be null
     */
    public static void isValid(SourceRecord record) {
        //print(record);

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
            } else {
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
            } else {
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

            // Make sure the schema names are valid Avro schema names ...
            validateSchemaNames(record.keySchema());
            validateSchemaNames(record.valueSchema());

            // Serialize and deserialize the key using the Avro converter, and check that we got the same result ...
            msg = "serializing key using Avro converter";
            byte[] avroKeyBytes = avroValueConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            msg = "deserializing key using Avro converter";
            avroKeyWithSchema = avroValueConverter.toConnectData(record.topic(), avroKeyBytes);
            msg = "comparing key schema to that serialized/deserialized with Avro converter";
            assertEquals(keyWithSchema.schema(), record.keySchema());
            msg = "comparing key to that serialized/deserialized with Avro converter";
            assertEquals(keyWithSchema.value(), record.key());
            msg = "comparing key to its schema";
            schemaMatchesStruct(keyWithSchema);

            // Serialize and deserialize the value using the Avro converter, and check that we got the same result ...
            msg = "serializing value using Avro converter";
            byte[] avroValueBytes = avroValueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using Avro converter";
            avroValueWithSchema = avroValueConverter.toConnectData(record.topic(), avroValueBytes);
            msg = "comparing value schema to that serialized/deserialized with Avro converter";
            assertEquals(valueWithSchema.schema(), record.valueSchema());
            msg = "comparing value to that serialized/deserialized with Avro converter";
            assertEquals(valueWithSchema.value(), record.value());
            msg = "comparing value to its schema";
            schemaMatchesStruct(valueWithSchema);

        } catch (Throwable t) {
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
            if (t instanceof AssertionError) throw t;
            fail("error " + msg + ": " + t.getMessage());
        }
    }

    protected static void validateSchemaNames(Schema schema) {
        if (schema == null) return;
        String schemaName = schema.name();
        if (schemaName != null && !AvroValidator.isValidFullname(schemaName)) {
            fail("Kafka schema '" + schemaName + "' is not a valid Avro schema name");
        }
        if (schema.type() == Type.STRUCT) {
            schema.fields().forEach(field -> {
                validateSubSchemaNames(schema, field);
            });
        }
    }

    protected static void validateSubSchemaNames(Schema parentSchema, Field field) {
        if (field == null) return;
        Schema subSchema = field.schema();
        String subSchemaName = subSchema.name();
        if (subSchemaName != null && !AvroValidator.isValidFullname(subSchemaName)) {
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
        } catch (Throwable t) {
            Testing.printError(t);
            Testing.print("Problem with message on topic '" + record.topic() + "':");
            if (keyJson != null) {
                Testing.print("valid key = " + prettyJson(keyJson));
            } else {
                Testing.print("invalid key");
            }
            if (valueJson != null) {
                Testing.print("valid value = " + prettyJson(valueJson));
            } else {
                Testing.print("invalid value");
            }
            fail(t.getMessage());
        }
    }

    protected static String prettyJson(JsonNode json) {
        try {
            return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (Throwable t) {
            Testing.printError(t);
            fail(t.getMessage());
            assert false : "Will not get here";
            return null;
        }
    }

    // The remaining methods are needed simply because of the KAFKA-3803, so our comparisons cannot rely upon Struct.equals

    protected static void assertEquals(Object o1, Object o2) {
        // assertThat(o1).isEqualTo(o2);
        if (!equals(o1, o2)) {
            fail(SchemaUtil.asString(o1) + " was not equal to " + SchemaUtil.asString(o2));
        }
    }

    @SuppressWarnings("unchecked")
    protected static boolean equals(Object o1, Object o2) {
        if (o1 == o2) return true;
        if (o1 == null) return o2 == null ? true : false;
        if (o2 == null) return false;
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
            if (!m1.keySet().equals(m2.keySet())) return false;
            for (Map.Entry<String, Object> entry : m1.entrySet()) {
                Object v1 = entry.getValue();
                Object v2 = m2.get(entry.getKey());
                if (!equals(v1, v2)) return false;
            }
            return true;
        }
        if (o1 instanceof Collection && o2 instanceof Collection) {
            Collection<Object> m1 = (Collection<Object>) o1;
            Collection<Object> m2 = (Collection<Object>) o2;
            if (m1.size() != m2.size()) return false;
            Iterator<?> iter1 = m1.iterator();
            Iterator<?> iter2 = m2.iterator();
            while (iter1.hasNext() && iter2.hasNext()) {
                if (!equals(iter1.next(), iter2.next())) return false;
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
            if (!Objects.equals(struct1.schema(), struct2.schema())) {
                return false;
            }
            Object[] array1 = valuesFor(struct1);
            Object[] array2 = valuesFor(struct2);
            boolean result = deepEquals(array1, array2);
            return result;
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
        if (a1 == a2)
            return true;
        if (a1 == null || a2 == null)
            return false;
        int length = a1.length;
        if (a2.length != length)
            return false;

        for (int i = 0; i < length; i++) {
            Object e1 = a1[i];
            Object e2 = a2[i];

            if (e1 == e2)
                continue;
            if (e1 == null)
                return false;

            // Figure out whether the two elements are equal
            boolean eq = deepEquals0(e1, e2);

            if (!eq)
                return false;
        }
        return true;
    }

    private static boolean deepEquals0(Object e1, Object e2) {
        assert e1 != null;
        boolean eq;
        if (e1 instanceof Object[] && e2 instanceof Object[])
            eq = deepEquals((Object[]) e1, (Object[]) e2);
        else if (e1 instanceof byte[] && e2 instanceof byte[])
            eq = Arrays.equals((byte[]) e1, (byte[]) e2);
        else if (e1 instanceof short[] && e2 instanceof short[])
            eq = Arrays.equals((short[]) e1, (short[]) e2);
        else if (e1 instanceof int[] && e2 instanceof int[])
            eq = Arrays.equals((int[]) e1, (int[]) e2);
        else if (e1 instanceof long[] && e2 instanceof long[])
            eq = Arrays.equals((long[]) e1, (long[]) e2);
        else if (e1 instanceof char[] && e2 instanceof char[])
            eq = Arrays.equals((char[]) e1, (char[]) e2);
        else if (e1 instanceof float[] && e2 instanceof float[])
            eq = Arrays.equals((float[]) e1, (float[]) e2);
        else if (e1 instanceof double[] && e2 instanceof double[])
            eq = Arrays.equals((double[]) e1, (double[]) e2);
        else if (e1 instanceof boolean[] && e2 instanceof boolean[])
            eq = Arrays.equals((boolean[]) e1, (boolean[]) e2);
        else
            eq = equals(e1, e2);
        return eq;
    }
}

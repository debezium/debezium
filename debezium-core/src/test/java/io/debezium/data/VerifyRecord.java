/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
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
        Map<String,Object> config = new HashMap<>();
        config.put("schemas.enable",Boolean.TRUE.toString());
        config.put("schemas.cache.size",100);
        keyJsonConverter.configure(config, true);
        keyJsonDeserializer.configure(config, true);
        valueJsonConverter.configure(config, false);
        valueJsonDeserializer.configure(config, false);

        config = new HashMap<>();
        config.put("schema.registry.url","http://fake-url");
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
     * @param value the value with a schema; may not be null
     */
    public static void schemaMatchesStruct(SchemaAndValue value) {
        Object val = value.value();
        assertThat(val).isInstanceOf(Struct.class);
        fieldsInSchema((Struct) val, value.schema());
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
        print(record);

        JsonNode keyJson = null;
        JsonNode valueJson = null;
        SchemaAndValue keyWithSchema = null;
        SchemaAndValue valueWithSchema = null;
        SchemaAndValue avroKeyWithSchema = null;
        SchemaAndValue avroValueWithSchema = null;
        try {
            // The key should never be null ...
            assertThat(record.key()).isNotNull();
            assertThat(record.keySchema()).isNotNull();

            // If the value is not null there must be a schema; otherwise, the schema should also be null ...
            if (record.value() == null) {
                assertThat(record.valueSchema()).isNull();
            } else {
                assertThat(record.valueSchema()).isNotNull();
            }

            // First serialize and deserialize the key ...
            byte[] keyBytes = keyJsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            keyJson = keyJsonDeserializer.deserialize(record.topic(), keyBytes);
            keyWithSchema = keyJsonConverter.toConnectData(record.topic(), keyBytes);
            assertThat(keyWithSchema.schema()).isEqualTo(record.keySchema());
            assertThat(keyWithSchema.value()).isEqualTo(record.key());
            schemaMatchesStruct(keyWithSchema);

            // then the value ...
            byte[] valueBytes = valueJsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            valueJson = valueJsonDeserializer.deserialize(record.topic(), valueBytes);
            valueWithSchema = valueJsonConverter.toConnectData(record.topic(), valueBytes);
            assertThat(valueWithSchema.schema()).isEqualTo(record.valueSchema());
            assertThat(valueWithSchema.value()).isEqualTo(record.value());
            schemaMatchesStruct(valueWithSchema);
            
            // Serialize and deserialize the key using the Avro converter, and check that we got the same result ...
            byte[] avroKeyBytes = avroValueConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
            avroKeyWithSchema = avroValueConverter.toConnectData(record.topic(), avroKeyBytes);
            assertThat(keyWithSchema.schema()).isEqualTo(record.keySchema());
            assertThat(keyWithSchema.value()).isEqualTo(record.key());
            schemaMatchesStruct(keyWithSchema);

            // Serialize and deserialize the value using the Avro converter, and check that we got the same result ...
            byte[] avroValueBytes = avroValueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            avroValueWithSchema = avroValueConverter.toConnectData(record.topic(), avroValueBytes);
            assertThat(valueWithSchema.schema()).isEqualTo(record.valueSchema());
            assertThat(valueWithSchema.value()).isEqualTo(record.value());
            schemaMatchesStruct(valueWithSchema);
            
        } catch (Throwable t) {
            Testing.Print.enable();
            Testing.print("Problem with message on topic '" + record.topic() + "':");
            Testing.printError(t);
            if (keyJson == null ){
                Testing.print("error deserializing key from JSON: " + SchemaUtil.asString(record.key()));
            } else if (keyWithSchema == null ){
                Testing.print("error using JSON converter on key: " + prettyJson(keyJson));
            } else if (avroKeyWithSchema == null ){
                Testing.print("error using Avro converter on key: " + prettyJson(keyJson));
            } else {
                Testing.print("valid key = " + prettyJson(keyJson));
            }

            if (valueJson == null ){
                Testing.print("error deserializing value from JSON: " + SchemaUtil.asString(record.value()));
            } else if (valueWithSchema == null ){
                Testing.print("error using JSON converter on value: " + prettyJson(valueJson));
            } else if (avroValueWithSchema == null ){
                Testing.print("error using Avro converter on value: " + prettyJson(valueJson));
            } else {
                Testing.print("valid key = " + prettyJson(keyJson));
            }
            if (t instanceof AssertionError) throw t;
            fail(t.getMessage());
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

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.debezium.converters.CloudEventsConverter;
import io.debezium.converters.CloudEventsMaker;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaUtil;
import io.debezium.util.Testing;

public class CloudEventsConverterTest {

    private static final CloudEventsConverter valueCEJsonConverter = new CloudEventsConverter();
    private static final JsonDeserializer valueJsonDeserializer = new JsonDeserializer();

    private static final MockSchemaRegistryClient ceSchemaRegistry = new MockSchemaRegistryClient();
    private static final CloudEventsConverter valueCEAvroConverter = new CloudEventsConverter();

    static {
        Map<String, Object> jsonConfig = new HashMap<>();
        jsonConfig.put("cloudevents.serializer.type", "json");
        jsonConfig.put("cloudevents.data.serializer.type", "json");
        jsonConfig.put("json.schemas.enable", Boolean.TRUE.toString());
        jsonConfig.put("schemas.cache.size", String.valueOf(100));
        valueCEJsonConverter.configure(jsonConfig, false);
        valueJsonDeserializer.configure(jsonConfig, false);

        Map<String, Object> avroConfig = new HashMap<>();
        avroConfig.put("cloudevents.serializer.type", "avro");
        avroConfig.put("cloudevents.data.serializer.type", "avro");
        avroConfig.put("schema.registry.url", "http://fake-url");
        valueCEAvroConverter.setCESchemaRegistry(ceSchemaRegistry);
        valueCEAvroConverter.configure(avroConfig, false);
    }

    public static void shouldConvertToCloudEventsInJson(SourceRecord record) throws IOException {
        JsonNode valueJson = null;
        JsonNode dataJson;
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

            // Convert the value and inspect it ...
            msg = "converting value using CloudEvents JSON converter";
            byte[] valueBytes = valueCEJsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using JSON deserializer";
            valueJson = valueJsonDeserializer.deserialize(record.topic(), valueBytes);
            msg = "inspecting all required CloudEvents fields in the value";
            assertThat(valueJson.get(CloudEventsMaker.FieldName.ID)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SOURCE)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SPECVERSION)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATASCHEMA)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATACONTENTTYPE)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.EXTRAINFO)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATA)).isNotNull();
            msg = "inspecting the extrainfo field in the value";
            assertThat(valueJson.get(CloudEventsMaker.FieldName.EXTRAINFO).get(Envelope.FieldName.OPERATION)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.EXTRAINFO).get(Envelope.FieldName.TIMESTAMP)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.EXTRAINFO).get(Envelope.FieldName.SOURCE)).isNotNull();
            msg = "inspecting the data field in the value";
            dataJson = valueJson.get(CloudEventsMaker.FieldName.DATA);
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME)).isNotNull();
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME)).isNotNull();
            // before field may be null
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME).get(Envelope.FieldName.AFTER)).isNotNull();
        }
        catch (Throwable t) {
            Testing.Print.enable();
            Testing.print("Problem with message on topic '" + record.topic() + "':");
            Testing.printError(t);
            Testing.print("error " + msg);
            Testing.print("  value: " + SchemaUtil.asString(record.value()));
            Testing.print("  value deserialized from CloudEvents in JSON: " + prettyJson(valueJson));
            if (t instanceof AssertionError) {
                throw t;
            }
            fail("error " + msg + ": " + t.getMessage());
        }
    }

    public static void shouldConvertToCloudEventsInAvro(SourceRecord record) {
        SchemaAndValue avroSchemaAndValue = null;
        String msg = null;
        Struct avroValue;

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

            // Convert the value and inspect it ...
            msg = "converting value using CloudEvents Avro converter";
            byte[] valueBytes = valueCEAvroConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using Avro deserializer";
            avroSchemaAndValue = valueCEAvroConverter.toConnectData(record.topic(), valueBytes);
            msg = "inspecting all required CloudEvents fields in the value";
            avroValue = (Struct) avroSchemaAndValue.value();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.ID)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.SOURCE)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.SPECVERSION)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.TYPE)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.DATACONTENTTYPE)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.DATASCHEMA)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.EXTRAINFO)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.DATA)).isNotNull();
            msg = "inspecting the extrainfo field in the value";
            Struct avroStruct = avroValue.getStruct(CloudEventsMaker.FieldName.EXTRAINFO);
            assertThat(avroStruct.get(Envelope.FieldName.OPERATION)).isNotNull();
            assertThat(avroStruct.get(Envelope.FieldName.TIMESTAMP)).isNotNull();
            assertThat(avroStruct.get(Envelope.FieldName.SOURCE)).isNotNull();
            msg = "inspecting the data field in the value";
            Struct avroDataField = avroValue.getStruct(CloudEventsMaker.FieldName.DATA);
            // before field may be null
            assertThat(avroDataField.schema().field(Envelope.FieldName.AFTER)).isNotNull();
        }
        catch (Throwable t) {
            Testing.Print.enable();
            Testing.print("Problem with message on topic '" + record.topic() + "':");
            Testing.printError(t);
            Testing.print("error " + msg);
            Testing.print("  value: " + SchemaUtil.asString(record.value()));
            if (avroSchemaAndValue != null) {
                Testing.print("  value to/from Avro: " + SchemaUtil.asString(avroSchemaAndValue.value()));
            }
            if (t instanceof AssertionError) {
                throw t;
            }
            fail("error " + msg + ": " + t.getMessage());
        }
    }

    private static String prettyJson(JsonNode json) {
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
}

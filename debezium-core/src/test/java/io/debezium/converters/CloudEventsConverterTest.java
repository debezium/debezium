/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.debezium.config.Configuration;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaUtil;
import io.debezium.util.Testing;

public class CloudEventsConverterTest {

    public static void shouldConvertToCloudEventsInJson(SourceRecord record, boolean hasTransaction) {
        shouldConvertToCloudEventsInJson(record, hasTransaction, valueJson -> {
        });
    }

    public static void shouldConvertToCloudEventsInJson(SourceRecord record, boolean hasTransaction, Consumer<JsonNode> furtherAssertions) {
        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "json");
        config.put("data.serializer.type", "json");
        config.put("json.schemas.enable", Boolean.TRUE.toString());
        config.put("json.schemas.cache.size", String.valueOf(100));

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter();
        cloudEventsConverter.configure(config, false);

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
            byte[] valueBytes = cloudEventsConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using CE deserializer";
            final SchemaAndValue ceValue = cloudEventsConverter.toConnectData(record.topic(), valueBytes);
            msg = "deserializing value using JSON deserializer";

            try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
                jsonDeserializer.configure(Collections.emptyMap(), false);
                valueJson = jsonDeserializer.deserialize(record.topic(), valueBytes);
            }

            msg = "inspecting all required CloudEvents fields in the value";
            assertThat(valueJson.get(CloudEventsMaker.FieldName.ID)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SOURCE)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SPECVERSION)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATASCHEMA)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).startsWith("io.debezium.connector.");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).endsWith(".DataChangeEvent");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TRACE_PARENT)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATACONTENTTYPE)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATA)).isNotNull();
            msg = "inspecting required CloudEvents extension attributes for Debezium";
            assertThat(valueJson.get("iodebeziumop")).isNotNull();
            assertThat(valueJson.get("iodebeziumtsms")).isNotNull();
            if (hasTransaction) {
                msg = "inspecting transaction metadata attributes";
                assertThat(valueJson.get("iodebeziumtxid")).isNotNull();
                assertThat(valueJson.get("iodebeziumtxtotalorder")).isNotNull();
                assertThat(valueJson.get("iodebeziumtxdatacollectionorder")).isNotNull();
            }
            msg = "inspecting the data field in the value";
            dataJson = valueJson.get(CloudEventsMaker.FieldName.DATA);
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME)).isNotNull();
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME).get("name").asText()).startsWith("io.debezium.connector.");
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME).get("name").asText()).endsWith(".Data");
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME)).isNotNull();
            // before field may be null
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME).get(Envelope.FieldName.AFTER)).isNotNull();

            furtherAssertions.accept(valueJson);
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

    public static void shouldConvertToCloudEventsInJsonWithDataAsAvro(SourceRecord record, boolean hasTransaction) {
        shouldConvertToCloudEventsInJsonWithDataAsAvro(record, "after", hasTransaction);
    }

    public static void shouldConvertToCloudEventsInJsonWithDataAsAvro(SourceRecord record, String fieldName, boolean hasTransaction) {
        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "json");
        config.put("data.serializer.type", "avro");
        config.put("avro.schema.registry.url", "http://fake-url");

        MockSchemaRegistryClient ceSchemaRegistry = new MockSchemaRegistryClient();
        Converter avroConverter = new AvroConverter(ceSchemaRegistry);
        avroConverter.configure(Configuration.from(config).subset("avro", true).asMap(), false);

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter(avroConverter);
        cloudEventsConverter.configure(config, false);

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
            byte[] valueBytes = cloudEventsConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using CE deserializer";
            final SchemaAndValue ceValue = cloudEventsConverter.toConnectData(record.topic(), valueBytes);
            msg = "deserializing value using JSON deserializer";

            try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
                jsonDeserializer.configure(Collections.emptyMap(), false);
                valueJson = jsonDeserializer.deserialize(record.topic(), valueBytes);
            }

            msg = "inspecting all required CloudEvents fields in the value";
            assertThat(valueJson.get(CloudEventsMaker.FieldName.ID)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SOURCE)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SPECVERSION)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATACONTENTTYPE).asText()).isEqualTo("application/avro");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATASCHEMA).asText()).startsWith("http://fake-url/schemas/ids/");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).startsWith("io.debezium.connector.");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).endsWith(".DataChangeEvent");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TRACE_PARENT)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATA)).isNotNull();
            msg = "inspecting required CloudEvents extension attributes for Debezium";
            assertThat(valueJson.get("iodebeziumop")).isNotNull();
            assertThat(valueJson.get("iodebeziumtsms")).isNotNull();
            if (hasTransaction) {
                msg = "inspecting transaction metadata attributes";
                assertThat(valueJson.get("iodebeziumtxid")).isNotNull();
                assertThat(valueJson.get("iodebeziumtxtotalorder")).isNotNull();
                assertThat(valueJson.get("iodebeziumtxdatacollectionorder")).isNotNull();
            }
            msg = "inspecting the data field in the value";
            dataJson = valueJson.get(CloudEventsMaker.FieldName.DATA);
            assertThat(dataJson).isNotNull();

            avroConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
            SchemaAndValue data = avroConverter.toConnectData(record.topic(), Base64.getDecoder().decode(dataJson.asText()));
            assertThat(data.schema().name()).startsWith("io.debezium.connector.");
            assertThat(data.schema().name()).endsWith(".Data");
            assertThat(data.value()).isInstanceOf(Struct.class);
            assertThat(((Struct) data.value()).get(fieldName)).describedAs("Field must be set: " + fieldName)
                    .isNotNull();
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

    public static void shouldConvertToCloudEventsInAvro(SourceRecord record, String connectorName, String serverName, boolean hasTransaction) {
        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "avro");
        config.put("data.serializer.type", "avro");
        config.put("avro.schema.registry.url", "http://fake-url");

        MockSchemaRegistryClient ceSchemaRegistry = new MockSchemaRegistryClient();
        AvroConverter avroConverter = new AvroConverter(ceSchemaRegistry);
        avroConverter.configure(Configuration.from(config).subset("avro", true).asMap(), false);

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter(avroConverter);
        cloudEventsConverter.configure(config, false);

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
            byte[] valueBytes = cloudEventsConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using Avro deserializer";
            avroSchemaAndValue = cloudEventsConverter.toConnectData(record.topic(), valueBytes);
            msg = "inspecting all required CloudEvents fields in the value";
            avroValue = (Struct) avroSchemaAndValue.value();
            assertThat(avroValue.schema().name()).startsWith(serverName + ".");
            assertThat(avroValue.schema().name()).endsWith(".CloudEvents.Envelope");
            assertThat(avroValue.get(CloudEventsMaker.FieldName.ID)).isNotNull();
            assertThat(avroValue.getString(CloudEventsMaker.FieldName.SOURCE)).isEqualTo("/debezium/" + connectorName + "/" + serverName);
            assertThat(avroValue.get(CloudEventsMaker.FieldName.SPECVERSION)).isEqualTo("1.0");
            assertThat(avroValue.get(CloudEventsMaker.FieldName.TYPE)).isEqualTo("io.debezium.connector." + connectorName + ".DataChangeEvent");
            assertThat(avroValue.get(CloudEventsMaker.FieldName.DATACONTENTTYPE)).isEqualTo("application/avro");
            assertThat(avroValue.getString(CloudEventsMaker.FieldName.DATASCHEMA)).startsWith("http://fake-url/schemas/ids/");
            assertThat(avroValue.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(avroValue.get(CloudEventsMaker.FieldName.DATA)).isNotNull();
            msg = "inspecting required CloudEvents extension attributes in the value";
            assertThat(avroValue.get(CloudEventsConverter.adjustExtensionName(Envelope.FieldName.OPERATION))).isNotNull();
            assertThat(avroValue.get(CloudEventsConverter.adjustExtensionName(Envelope.FieldName.TIMESTAMP))).isNotNull();
            if (hasTransaction) {
                msg = "inspecting transaction metadata attributes";
                assertThat(avroValue.get("iodebeziumtxid")).isNotNull();
                assertThat(avroValue.get("iodebeziumtxtotalorder")).isNotNull();
                assertThat(avroValue.get("iodebeziumtxdatacollectionorder")).isNotNull();
            }
            msg = "inspecting the data field in the value";
            Struct avroDataField = avroValue.getStruct(CloudEventsMaker.FieldName.DATA);
            assertThat(avroDataField.schema().name()).startsWith("io.debezium.connector." + connectorName + ".Data");
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

    public static void shouldConvertToCloudEventsInJsonWithMetadataAndIdAndTypeInHeaders(SourceRecord record, String connectorName, String serverName) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "json");
        config.put("data.serializer.type", "json");
        config.put("metadata.source", "header");

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter();
        cloudEventsConverter.configure(config, false);

        JsonNode valueJson = null;
        JsonNode dataJson;
        String msg = null;

        try {
            // Convert the value and inspect it ...
            msg = "converting value using CloudEvents JSON converter";
            byte[] valueBytes = cloudEventsConverter.fromConnectData(record.topic(), convertHeadersFor(record), record.valueSchema(), record.value());
            msg = "deserializing value using CE deserializer";
            final SchemaAndValue ceValue = cloudEventsConverter.toConnectData(record.topic(), valueBytes);
            msg = "deserializing value using JSON deserializer";

            try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
                jsonDeserializer.configure(Collections.emptyMap(), false);
                valueJson = jsonDeserializer.deserialize(record.topic(), valueBytes);
            }

            msg = "inspecting all required CloudEvents fields in the value";
            assertThat(valueJson.get(CloudEventsMaker.FieldName.ID).asText()).isEqualTo("59a42efd-b015-44a9-9dde-cb36d9002425");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SOURCE).asText()).isEqualTo("/debezium/" + connectorName + "/" + serverName);
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SPECVERSION).asText()).isEqualTo("1.0");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATASCHEMA)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).isEqualTo("UserCreated");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TRACE_PARENT)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATACONTENTTYPE).asText()).isEqualTo("application/json");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATA)).isNotNull();
            msg = "inspecting required CloudEvents extension attributes for Debezium";
            assertThat(valueJson.get("iodebeziumop")).isNotNull();
            assertThat(valueJson.get("iodebeziumtsms")).isNotNull();
            msg = "inspecting transaction metadata attributes";
            assertThat(valueJson.get("iodebeziumtxid")).isNotNull();
            assertThat(valueJson.get("iodebeziumtxtotalorder")).isNotNull();
            assertThat(valueJson.get("iodebeziumtxdatacollectionorder")).isNotNull();
            msg = "inspecting the data field in the value";
            dataJson = valueJson.get(CloudEventsMaker.FieldName.DATA);
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME)).isNotNull();
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME).get("name").asText()).isEqualTo("io.debezium.connector." + connectorName + ".Data");
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME)).isNotNull();
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME).get("someField1").textValue()).isEqualTo("some value 1");
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME).get("someField2").intValue()).isEqualTo(7005);
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

    public static void shouldConvertToCloudEventsInJsonWithDataAsAvroAndAllMetadataInHeaders(SourceRecord record, String connectorName, String serverName)
            throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("data.serializer.type", "avro");
        config.put("avro.schema.registry.url", "http://fake-url");
        config.put("metadata.source", "header");
        config.put("opentelemetry.tracing.attributes.enable", true);
        config.put("schema.data.name.source.header.enable", true);

        MockSchemaRegistryClient ceSchemaRegistry = new MockSchemaRegistryClient();
        Converter avroConverter = new AvroConverter(ceSchemaRegistry);
        avroConverter.configure(Configuration.from(config).subset("avro", true).asMap(), false);

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter(avroConverter);
        cloudEventsConverter.configure(config, false);

        JsonNode valueJson = null;
        JsonNode dataJson;
        String msg = null;

        try {
            // Convert the value and inspect it ...
            msg = "converting value using CloudEvents JSON converter";
            byte[] valueBytes = cloudEventsConverter.fromConnectData(record.topic(), convertHeadersFor(record), record.valueSchema(), record.value());
            msg = "deserializing value using CE deserializer";
            final SchemaAndValue ceValue = cloudEventsConverter.toConnectData(record.topic(), valueBytes);
            msg = "deserializing value using JSON deserializer";

            try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
                jsonDeserializer.configure(Collections.emptyMap(), false);
                valueJson = jsonDeserializer.deserialize(record.topic(), valueBytes);
            }

            msg = "inspecting all required CloudEvents fields in the value";
            assertThat(valueJson.get(CloudEventsMaker.FieldName.ID).asText()).isEqualTo("59a42efd-b015-44a9-9dde-cb36d9002425");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SOURCE).asText()).isEqualTo("/debezium/" + connectorName + "/" + serverName);
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SPECVERSION).asText()).isEqualTo("1.0");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATASCHEMA).asText()).startsWith("http://fake-url/schemas/ids/");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).isEqualTo("UserCreated");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TRACE_PARENT).asText()).startsWith("00-f99aefa4b9c40a436432b62f851a8159-");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TRACE_PARENT).asText()).endsWith("-01");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATACONTENTTYPE).asText()).isEqualTo("application/avro");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATA)).isNotNull();
            msg = "inspecting required CloudEvents extension attributes for Debezium";
            assertThat(valueJson.get("iodebeziumop")).isNotNull();
            assertThat(valueJson.get("iodebeziumtsms")).isNotNull();
            msg = "inspecting transaction metadata attributes";
            assertThat(valueJson.get("iodebeziumtxid")).isNotNull();
            assertThat(valueJson.get("iodebeziumtxtotalorder")).isNotNull();
            assertThat(valueJson.get("iodebeziumtxdatacollectionorder")).isNotNull();
            msg = "inspecting the data field in the value";
            dataJson = valueJson.get(CloudEventsMaker.FieldName.DATA);
            assertThat(dataJson).isNotNull();
            avroConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
            SchemaAndValue data = avroConverter.toConnectData(record.topic(), Base64.getDecoder().decode(dataJson.asText()));
            // checking custom data schema name
            assertThat(data.schema().name()).isEqualTo("User");
            assertThat(data.value()).isInstanceOf(Struct.class);
            Struct dataValue = (Struct) data.value();
            assertThat((String) (dataValue.get("someField1"))).isEqualTo("some value 1");
            assertThat(dataValue.getInt32("someField2")).isEqualTo(7005);
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

    public static void shouldConvertToCloudEventsInJsonWithIdFromHeaderAndGeneratedType(SourceRecord record, String connectorName, String serverName) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "json");
        config.put("data.serializer.type", "json");
        config.put("metadata.source", "value,id:header,type:generate");

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter();
        cloudEventsConverter.configure(config, false);

        JsonNode valueJson = null;
        JsonNode dataJson;
        String msg = null;

        try {
            // Convert the value and inspect it ...
            msg = "converting value using CloudEvents JSON converter";
            byte[] valueBytes = cloudEventsConverter.fromConnectData(record.topic(), convertHeadersFor(record), record.valueSchema(), record.value());
            msg = "deserializing value using CE deserializer";
            final SchemaAndValue ceValue = cloudEventsConverter.toConnectData(record.topic(), valueBytes);
            msg = "deserializing value using JSON deserializer";

            try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
                jsonDeserializer.configure(Collections.emptyMap(), false);
                valueJson = jsonDeserializer.deserialize(record.topic(), valueBytes);
            }

            msg = "inspecting all required CloudEvents fields in the value";
            assertThat(valueJson.get(CloudEventsMaker.FieldName.ID).asText()).isEqualTo("77742efd-b015-44a9-9dde-cb36d9002425");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SOURCE).asText()).isEqualTo("/debezium/" + connectorName + "/" + serverName);
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SPECVERSION).asText()).isEqualTo("1.0");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATASCHEMA)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).isEqualTo("io.debezium.connector." + connectorName + ".DataChangeEvent");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TRACE_PARENT)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATACONTENTTYPE).asText()).isEqualTo("application/json");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATA)).isNotNull();
            msg = "inspecting required CloudEvents extension attributes for Debezium";
            assertThat(valueJson.get("iodebeziumop")).isNotNull();
            assertThat(valueJson.get("iodebeziumtsms")).isNotNull();
            msg = "inspecting transaction metadata attributes";
            assertThat(valueJson.get("iodebeziumtxid")).isNotNull();
            assertThat(valueJson.get("iodebeziumtxtotalorder")).isNotNull();
            assertThat(valueJson.get("iodebeziumtxdatacollectionorder")).isNotNull();
            msg = "inspecting the data field in the value";
            dataJson = valueJson.get(CloudEventsMaker.FieldName.DATA);
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME)).isNotNull();
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME).get("name").asText()).isEqualTo("io.debezium.connector." + connectorName + ".Data");
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

    public static void shouldConvertToCloudEventsInJsonWithoutExtensionAttributes(SourceRecord record) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "json");
        config.put("data.serializer.type", "json");
        config.put("extension.attributes.enable", false);

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter();
        cloudEventsConverter.configure(config, false);

        JsonNode valueJson = null;
        JsonNode dataJson;
        String msg = null;

        try {
            // Convert the value and inspect it ...
            msg = "converting value using CloudEvents JSON converter";
            byte[] valueBytes = cloudEventsConverter.fromConnectData(record.topic(), convertHeadersFor(record), record.valueSchema(), record.value());
            msg = "deserializing value using CE deserializer";
            final SchemaAndValue ceValue = cloudEventsConverter.toConnectData(record.topic(), valueBytes);
            msg = "deserializing value using JSON deserializer";

            try (JsonDeserializer jsonDeserializer = new JsonDeserializer()) {
                jsonDeserializer.configure(Collections.emptyMap(), false);
                valueJson = jsonDeserializer.deserialize(record.topic(), valueBytes);
            }

            msg = "inspecting all required CloudEvents fields in the value";
            assertThat(valueJson.get(CloudEventsMaker.FieldName.ID)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SOURCE).asText()).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.SPECVERSION).asText()).isEqualTo("1.0");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATASCHEMA)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).startsWith("io.debezium.connector.");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TYPE).asText()).endsWith(".DataChangeEvent");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TRACE_PARENT)).isNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATACONTENTTYPE).asText()).isEqualTo("application/json");
            assertThat(valueJson.get(CloudEventsMaker.FieldName.TIME)).isNotNull();
            assertThat(valueJson.get(CloudEventsMaker.FieldName.DATA)).isNotNull();

            msg = "inspecting the data field in the value";
            dataJson = valueJson.get(CloudEventsMaker.FieldName.DATA);
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME)).isNotNull();
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME).get("name").asText()).startsWith("io.debezium.connector.");
            assertThat(dataJson.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME).get("name").asText()).endsWith(".Data");
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME)).isNotNull();
            // before field may be null
            assertThat(dataJson.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME).get(Envelope.FieldName.AFTER)).isNotNull();

            assertThat(valueJson.fieldNames()).toIterable().noneMatch(fieldName -> fieldName.startsWith("iodebezium"));
            // a cloud event should contain only "basic" attributes the number of which is 7
            assertThat(Iterators.size(valueJson.fields())).isEqualTo(7);
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

    public static void shouldThrowExceptionWhenDeserializingNotCloudEventJson(SourceRecord record) throws Exception {
        Map<String, Object> jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        jsonConverterConfig.put(JsonConverterConfig.TYPE_CONFIG, "value");

        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(jsonConverterConfig);

        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "json");
        config.put("data.serializer.type", "json");

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter();
        cloudEventsConverter.configure(config, false);

        JsonNode valueJson = null;
        String msg = null;

        try {
            msg = "converting value using wrong - plain JSON - converter";
            byte[] valueBytes = jsonConverter.fromConnectData(record.topic(), convertHeadersFor(record), record.valueSchema(), record.value());

            Exception exception = assertThrows(DataException.class, () -> cloudEventsConverter.toConnectData(record.topic(), valueBytes));

            assertThat(exception.getMessage()).startsWith("A deserialized record's value is not a CloudEvent: value={");
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

    public static void shouldThrowExceptionWhenDeserializingNotCloudEventAvro(SourceRecord record) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "avro");
        config.put("data.serializer.type", "avro");
        config.put("avro.schema.registry.url", "http://fake-url");

        MockSchemaRegistryClient ceSchemaRegistry = new MockSchemaRegistryClient();
        AvroConverter avroConverter = new AvroConverter(ceSchemaRegistry);
        avroConverter.configure(Configuration.from(config).subset("avro", true).asMap(), false);

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter(avroConverter);
        cloudEventsConverter.configure(config, false);

        JsonNode valueJson = null;
        String msg = null;

        try {
            msg = "converting value using wrong - plain Avro - converter";
            byte[] valueBytes = avroConverter.fromConnectData(record.topic(), convertHeadersFor(record), record.valueSchema(), record.value());

            Exception exception = assertThrows(DataException.class, () -> cloudEventsConverter.toConnectData(record.topic(), valueBytes));

            assertThat(exception.getMessage()).startsWith("A deserialized record's value is not a CloudEvent: value=Struct{");
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

    public static void shouldConvertToCloudEventsInAvroWithCustomCloudEventsSchemaName(SourceRecord record) {
        Map<String, Object> config = new HashMap<>();
        config.put("serializer.type", "avro");
        config.put("data.serializer.type", "avro");
        config.put("avro.schema.registry.url", "http://fake-url");
        config.put("schema.cloudevents.name", "TestSchemaName1");

        MockSchemaRegistryClient ceSchemaRegistry = new MockSchemaRegistryClient();
        AvroConverter avroConverter = new AvroConverter(ceSchemaRegistry);
        avroConverter.configure(Configuration.from(config).subset("avro", true).asMap(), false);

        CloudEventsConverter cloudEventsConverter = new CloudEventsConverter(avroConverter);
        cloudEventsConverter.configure(config, false);

        SchemaAndValue avroSchemaAndValue = null;
        String msg = null;

        try {
            // Convert the value and inspect it ...
            msg = "converting value using CloudEvents Avro converter";
            byte[] valueBytes = cloudEventsConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            msg = "deserializing value using Avro deserializer";
            avroSchemaAndValue = cloudEventsConverter.toConnectData(record.topic(), valueBytes);
            msg = "inspecting all required CloudEvents fields in the value";
            Schema avroSchema = avroSchemaAndValue.schema();
            assertThat(avroSchema.name()).isEqualTo("TestSchemaName1");
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

    private static RecordHeaders convertHeadersFor(SourceRecord record) throws IOException {
        try (HeaderConverter jsonHeaderConverter = new JsonConverter()) {
            Map<String, Object> jsonHeaderConverterConfig = new HashMap<>();
            jsonHeaderConverterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
            jsonHeaderConverterConfig.put(JsonConverterConfig.TYPE_CONFIG, "header");
            jsonHeaderConverter.configure(jsonHeaderConverterConfig);

            Headers headers = record.headers();
            RecordHeaders result = new RecordHeaders();
            if (headers != null) {
                String topic = record.topic();
                for (Header header : headers) {
                    String key = header.key();
                    byte[] rawHeader = jsonHeaderConverter.fromConnectHeader(topic, key, header.schema(), header.value());
                    result.add(key, rawHeader);
                }
            }
            return result;
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

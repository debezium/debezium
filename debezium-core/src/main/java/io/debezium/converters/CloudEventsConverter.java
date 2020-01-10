/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import static io.debezium.converters.SerializerType.withName;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.data.Envelope;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Implementation of Converter that express schemas and objects with CloudEvents specification. The serialization
 * format can be Json or Avro.
 * <p>
 * The serialization format of CloudEvents is configured with
 * {@link CloudEventsConverterConfig#CLOUDEVENTS_SERIALIZER_TYPE_CONFIG cloudevents.serializer.type} option.
 * <p>
 * The serialization format of the data attribute in CloudEvents is configured with
 * {@link CloudEventsConverterConfig#CLOUDEVENTS_DATA_SERIALIZER_TYPE_CONFIG cloudevents.data.serializer.type} option.
 * <p>
 * If the serialization format of the data attribute in CloudEvents is JSON, it can be configured whether to enable
 * schemas with {@link CloudEventsConverterConfig#CLOUDEVENTS_JSON_SCHEMAS_ENABLE_CONFIG json.schemas.enable} option.
 * <p>
 * If the the serialization format is Avro, the URL of schema registries for CloudEvents and its data attribute can be
 * configured with {@link CloudEventsConverterConfig#CLOUDEVENTS_SCHEMA_REGISTRY_URL_CONFIG schema.registry.url}.
 * <p>
 * There are two modes for transferring CloudEvents as Kafka messages: structured and binary. In the structured content
 * mode, event metadata attributes and event data are placed into the Kafka message value section using an event format.
 * In the binary content mode, the value of the event data is placed into the Kafka message's value section as-is,
 * with the content-type header value declaring its media type; all other event attributes are mapped to the Kafka
 * message's header section.
 * <p>
 * Since Kafka converters has not support headers yet, right now CloudEvents converter use structured mode as the
 * default.
 */
public class CloudEventsConverter implements Converter {

    private static final String EXTENSION_NAME_PREFIX = "iodebezium";

    private static final Logger LOGGER = LoggerFactory.getLogger(CloudEventsConverter.class);
    private static Method CONVERT_TO_JSON_METHOD;
    private static Method CONVERT_TO_CONNECT_METHOD;

    static {
        try {
            CONVERT_TO_JSON_METHOD = JsonConverter.class.getDeclaredMethod("convertToJson", Schema.class, Object.class);
            CONVERT_TO_CONNECT_METHOD = JsonConverter.class.getDeclaredMethod("convertToConnect", Schema.class, JsonNode.class);
            CONVERT_TO_JSON_METHOD.setAccessible(true);
            CONVERT_TO_CONNECT_METHOD.setAccessible(true);
        }
        catch (NoSuchMethodException e) {
            throw new DataException(e.getCause());
        }
    }

    private final JsonConverter jsonCEConverter = new JsonConverter();
    private final JsonSerializer jsonSerializer = new JsonSerializer();
    private final JsonDeserializer jsonDeserializer = new JsonDeserializer();

    private final SchemaRegistryClient schemaRegistry;
    private Converter avroConverter;

    private SerializerType ceSerializerType = withName(CloudEventsConverterConfig.CLOUDEVENTS_SERIALIZER_TYPE_DEFAULT);
    private SerializerType dataSerializerType = withName(CloudEventsConverterConfig.CLOUDEVENTS_DATA_SERIALIZER_TYPE_DEFAULT);
    private boolean enableJsonSchemas = CloudEventsConverterConfig.CLOUDEVENTS_JSON_SCHEMAS_ENABLE_DEFAULT;
    private List<String> schemaRegistryUrls;

    public CloudEventsConverter() {
        this(null);
    }

    public CloudEventsConverter(MockSchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        CloudEventsConverterConfig ceConfig = new CloudEventsConverterConfig(conf);
        ceSerializerType = ceConfig.cloudeventsSerializerType();
        dataSerializerType = ceConfig.cloudeventsDataSerializerTypeConfig();

        boolean needAvroConverter = false;

        if (ceSerializerType == SerializerType.JSON) {
            final JsonConverterConfig jsonConfig = new JsonConverterConfig(conf);
            conf.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, Boolean.FALSE.toString());
            conf.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, jsonConfig.schemaCacheSize());
            jsonCEConverter.configure(conf, isKey);
        }
        else {
            needAvroConverter = true;

            if (dataSerializerType == SerializerType.JSON) {
                throw new IllegalStateException("Cannot use 'application/json' data content type within Avro events");
            }
        }

        if (dataSerializerType == SerializerType.JSON) {
            enableJsonSchemas = ceConfig.cloudeventsJsonSchemasEnable();
        }
        else {
            needAvroConverter = true;
        }

        if (needAvroConverter) {
            avroConverter = new AvroConverter(schemaRegistry);

            schemaRegistryUrls = ceConfig.cloudeventsSchemaRegistryUrls();
            if (schemaRegistryUrls == null) {
                throw new DataException("Need url(s) for schema registry instances for CloudEvents");
            }
            final Map<String, Object> avroConfigs = new HashMap<>(conf);
            avroConfigs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, String.join(",", schemaRegistryUrls));
            avroConverter.configure(avroConfigs, false);
        }
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null || value == null) {
            return null;
        }
        if (schema.type() != STRUCT) {
            throw new DataException("Mismatching schema");
        }

        RecordParser parser = RecordParser.create(schema, value);
        CloudEventsMaker maker = CloudEventsMaker.create(parser, dataSerializerType,
                (schemaRegistryUrls == null) ? null : String.join(",", schemaRegistryUrls));

        if (ceSerializerType == SerializerType.JSON) {
            if (dataSerializerType == SerializerType.JSON) {
                // JSON - JSON (with schema in data)
                if (enableJsonSchemas) {
                    SchemaBuilder dummy = SchemaBuilder.struct();
                    SchemaAndValue cloudEvent = convertToCloudEventsFormat(parser, maker, dummy, null, new Struct(dummy));

                    // need to create a JSON node with schema + payload first
                    JsonNode serializedJsonData = convertToJsonNode(maker.ceDataAttributeSchema(), maker.ceDataAttribute(), enableJsonSchemas);

                    // replace the "data" with the schema + payload JSON node; the event itself must not have schema enabled,
                    // so to be a proper CloudEvent
                    ObjectNode cloudEventJson = (ObjectNode) convertToJsonNode(cloudEvent.schema(), cloudEvent.value(), false);
                    cloudEventJson.set(CloudEventsMaker.FieldName.DATA, serializedJsonData);

                    return jsonSerializer.serialize(topic, cloudEventJson);
                }
                // JSON - JSON (without schema); can just use the regular JSON converter for the entire event
                else {
                    SchemaAndValue cloudEvent = convertToCloudEventsFormat(parser, maker, maker.ceDataAttributeSchema(), null, maker.ceDataAttribute());
                    return jsonCEConverter.fromConnectData(topic, cloudEvent.schema(), cloudEvent.value());
                }
            }
            // JSON - Avro; need to convert "data" to Avro first
            else {
                SchemaAndValue cloudEvent = convertToCloudEventsFormatWithDataAsAvro(topic, parser, maker);
                return jsonCEConverter.fromConnectData(topic, cloudEvent.schema(), cloudEvent.value());
            }
        }
        // Avro - Avro; need to convert "data" to Avro first
        else {
            SchemaAndValue cloudEvent = convertToCloudEventsFormatWithDataAsAvro(topic, parser, maker);
            return avroConverter.fromConnectData(topic, cloudEvent.schema(), cloudEvent.value());
        }
    }

    /**
     * Creates a CloudEvents wrapper, converting the "data" to Avro.
     */
    private SchemaAndValue convertToCloudEventsFormatWithDataAsAvro(String topic, RecordParser parser, CloudEventsMaker maker) {
        Schema dataSchemaType = Schema.BYTES_SCHEMA;
        byte[] serializedData = avroConverter.fromConnectData(topic, maker.ceDataAttributeSchema(), maker.ceDataAttribute());
        String dataSchemaUri = maker.ceDataschemaUri(getSchemaIdFromAvroMessage(serializedData));

        return convertToCloudEventsFormat(parser, maker, dataSchemaType, dataSchemaUri, serializedData);
    }

    /**
     * Obtains the schema id from the given Avro record. They are prefixed by one magic byte,
     * followed by an int for the schem id.
     */
    private String getSchemaIdFromAvroMessage(byte[] serializedData) {
        return String.valueOf(ByteBuffer.wrap(serializedData, 1, 5).getInt());
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        switch (ceSerializerType) {
            case JSON:
                JsonNode jsonValue;

                try {
                    jsonValue = jsonDeserializer.deserialize(topic, value);
                    byte[] data = jsonValue.get(CloudEventsMaker.FieldName.DATA).binaryValue();
                    SchemaAndValue dataField = reconvertData(topic, data, dataSerializerType, enableJsonSchemas);
                    Schema incompleteSchema = jsonCEConverter.asConnectSchema(jsonValue);
                    SchemaBuilder builder = SchemaBuilder.struct();

                    for (Field ceField : incompleteSchema.fields()) {
                        if (ceField.name().equals(CloudEventsMaker.FieldName.DATA)) {
                            builder.field(ceField.name(), dataField.schema());
                        }
                        else {
                            builder.field(ceField.name(), ceField.schema());
                        }
                    }
                    builder.name(incompleteSchema.name());
                    builder.version(incompleteSchema.version());
                    builder.doc(incompleteSchema.doc());
                    for (Map.Entry<String, String> entry : incompleteSchema.parameters().entrySet()) {
                        builder.parameter(entry.getKey(), entry.getValue());
                    }
                    Schema schema = builder.build();

                    Struct incompleteStruct = (Struct) CONVERT_TO_CONNECT_METHOD.invoke(jsonCEConverter, incompleteSchema, jsonValue);
                    Struct struct = new Struct(schema);

                    for (Field ceField : incompleteSchema.fields()) {
                        if (ceField.name().equals(CloudEventsMaker.FieldName.DATA)) {
                            struct.put(ceField, dataField.value());
                        }
                        struct.put(ceField, incompleteStruct.get(ceField));
                    }

                    return new SchemaAndValue(schema, value);
                }
                catch (SerializationException | IOException | IllegalAccessException | InvocationTargetException e) {
                    throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
                }
            case AVRO:
                // First reconvert the whole CloudEvents
                // Then reconvert the "data" field
                SchemaAndValue ceSchemaAndValue = avroConverter.toConnectData(topic, value);
                Schema incompleteSchema = ceSchemaAndValue.schema();
                Struct ceValue = (Struct) ceSchemaAndValue.value();
                byte[] data = ceValue.getBytes(CloudEventsMaker.FieldName.DATA);
                SchemaAndValue dataSchemaAndValue = avroConverter.toConnectData(topic, data);
                SchemaBuilder builder = SchemaBuilder.struct();

                for (Field ceField : incompleteSchema.fields()) {
                    if (ceField.name().equals(CloudEventsMaker.FieldName.DATA)) {
                        builder.field(ceField.name(), dataSchemaAndValue.schema());
                    }
                    else {
                        builder.field(ceField.name(), ceField.schema());
                    }
                }
                builder.name(incompleteSchema.name());
                builder.version(incompleteSchema.version());
                builder.doc(incompleteSchema.doc());
                if (incompleteSchema.parameters() != null) {
                    for (Map.Entry<String, String> entry : incompleteSchema.parameters().entrySet()) {
                        builder.parameter(entry.getKey(), entry.getValue());
                    }
                }
                Schema schema = builder.build();

                Struct struct = new Struct(schema);
                for (Field field : schema.fields()) {
                    if (field.name().equals(CloudEventsMaker.FieldName.DATA)) {
                        struct.put(field, dataSchemaAndValue.value());
                    }
                    else {
                        struct.put(field, ceValue.get(field));
                    }
                }

                return new SchemaAndValue(schema, struct);
        }

        return SchemaAndValue.NULL;
    }

    private SchemaAndValue reconvertData(String topic, byte[] serializedData, SerializerType dataType, Boolean enableSchemas) {
        switch (dataType) {
            case JSON:
                JsonNode jsonValue;

                try {
                    jsonValue = jsonDeserializer.deserialize(topic, serializedData);
                }
                catch (SerializationException e) {
                    throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
                }

                if (!enableSchemas) {
                    ObjectNode envelope = JsonNodeFactory.instance.objectNode();
                    envelope.set(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME, null);
                    envelope.set(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME, jsonValue);
                    jsonValue = envelope;
                }

                Schema schema = jsonCEConverter.asConnectSchema(jsonValue.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME));

                try {
                    return new SchemaAndValue(
                            schema,
                            CONVERT_TO_CONNECT_METHOD.invoke(jsonCEConverter, schema, jsonValue.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME)));
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new DataException(e.getCause());
                }
            case AVRO:
                return avroConverter.toConnectData(topic, serializedData);
            default:
                throw new DataException("No such serializer for \"" + dataSerializerType + "\" format");
        }
    }

    private SchemaAndValue convertToCloudEventsFormat(RecordParser parser, CloudEventsMaker maker, Schema dataSchemaType, String dataSchema, Object serializedData) {
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(LOGGER);
        Struct source = parser.source();
        Schema sourceSchema = parser.source().schema();

        // construct schema of CloudEvents envelope
        CESchemaBuilder ceSchemaBuilder = defineSchema()
                .withName(schemaNameAdjuster.adjust(maker.ceEnvelopeSchemaName()))
                .withSchema(CloudEventsMaker.FieldName.ID, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.SOURCE, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.SPECVERSION, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.TYPE, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.TIME, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.DATACONTENTTYPE, Schema.STRING_SCHEMA);

        if (dataSchema != null) {
            ceSchemaBuilder.withSchema(CloudEventsMaker.FieldName.DATASCHEMA, Schema.STRING_SCHEMA);
        }

        ceSchemaBuilder.withSchema(adjustExtensionName(Envelope.FieldName.OPERATION), Schema.STRING_SCHEMA);

        for (Field field : sourceSchema.fields()) {
            ceSchemaBuilder.withSchema(adjustExtensionName(field.name()), convertToCeExtensionSchema(field.schema()));
        }

        ceSchemaBuilder.withSchema(CloudEventsMaker.FieldName.DATA, dataSchemaType);

        Schema ceSchema = ceSchemaBuilder.build();

        // construct value of CloudEvents Envelope
        CEValueBuilder ceValueBuilder = withValue(ceSchema)
                .withValue(CloudEventsMaker.FieldName.ID, maker.ceId())
                .withValue(CloudEventsMaker.FieldName.SOURCE, maker.ceSource(source.getString("name")))
                .withValue(CloudEventsMaker.FieldName.SPECVERSION, maker.ceSpecversion())
                .withValue(CloudEventsMaker.FieldName.TYPE, maker.ceType())
                .withValue(CloudEventsMaker.FieldName.TIME, maker.ceTime())
                .withValue(CloudEventsMaker.FieldName.DATACONTENTTYPE, maker.ceDatacontenttype());
        if (dataSchema != null) {
            ceValueBuilder.withValue(CloudEventsMaker.FieldName.DATASCHEMA, dataSchema);
        }

        ceValueBuilder.withValue(adjustExtensionName(Envelope.FieldName.OPERATION), parser.op());

        for (Field field : sourceSchema.fields()) {
            Object value = source.get(field);
            if (field.schema().type() == Type.INT64 && value != null) {
                value = String.valueOf((long) value);
            }
            ceValueBuilder.withValue(adjustExtensionName(field.name()), value);
        }

        ceValueBuilder.withValue(CloudEventsMaker.FieldName.DATA, serializedData);

        return new SchemaAndValue(ceSchema, ceValueBuilder.build());
    }

    /**
     * Converts the given source attribute schema into a corresponding CE extension schema.
     * The types supported there are limited, e.g. int64 can only be represented as string.
     */
    private Schema convertToCeExtensionSchema(Schema schema) {
        SchemaBuilder ceExtensionSchema;

        if (schema.type() == Type.BOOLEAN) {
            ceExtensionSchema = SchemaBuilder.bool();
        }
        // all numbers up to int32 go as int32
        else if (schema.type() == Type.INT8 || schema.type() == Type.INT16 || schema.type() == Type.INT16) {
            ceExtensionSchema = SchemaBuilder.int32();
        }
        // int64 isn't supported as per CE spec
        else if (schema.type() == Type.STRING || schema.type() == Type.INT64) {
            ceExtensionSchema = SchemaBuilder.string();
        }
        // further attribute types may be supported in the future, but the ones above are the ones
        // currently used in the "source" block of Debezium events
        else {
            throw new IllegalArgumentException("Source field of type " + schema.type() + " cannot be converted into CloudEvents extension attribute.");
        }

        if (schema.isOptional()) {
            ceExtensionSchema.optional();
        }

        return ceExtensionSchema.build();
    }

    private JsonNode convertToJsonNode(Schema schema, Object value, boolean enableJsonSchemas) {
        try {
            JsonNode withoutSchemaNode = (JsonNode) CONVERT_TO_JSON_METHOD.invoke(jsonCEConverter, schema, value);

            if (!enableJsonSchemas) {
                return withoutSchemaNode;
            }

            ObjectNode schemaNode = jsonCEConverter.asJsonSchema(schema);
            ObjectNode withSchemaNode = JsonNodeFactory.instance.objectNode();
            withSchemaNode.set(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME, schemaNode);
            withSchemaNode.set(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME, withoutSchemaNode);

            return withSchemaNode;
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new DataException(e.getCause());
        }
    }

    private static CESchemaBuilder defineSchema() {
        return new CESchemaBuilder() {
            private final SchemaBuilder builder = SchemaBuilder.struct();

            @Override
            public CESchemaBuilder withName(String name) {
                builder.name(name);
                return this;
            }

            @Override
            public CESchemaBuilder withSchema(String fieldName, Schema fieldSchema) {
                builder.field(fieldName, fieldSchema);
                return this;
            }

            @Override
            public Schema build() {
                return builder.build();
            }
        };
    }

    private static CEValueBuilder withValue(Schema schema) {
        return new CEValueBuilder() {
            private final Schema ceSchema = schema;
            private final Struct ceValue = new Struct(ceSchema);

            @Override
            public CEValueBuilder withValue(String fieldName, Object value) {
                if (ceSchema.field(fieldName) == null) {
                    throw new DataException(fieldName + " is not a valid field name");
                }

                ceValue.put(fieldName, value);
                return this;
            }

            @Override
            public Struct build() {
                return ceValue;
            }
        };
    }

    /**
     * Builder of a CloudEvents envelope schema.
     */
    public interface CESchemaBuilder {

        CESchemaBuilder withName(String name);

        CESchemaBuilder withSchema(String fieldName, Schema fieldSchema);

        Schema build();
    }

    /**
     * Builder of a CloudEvents value.
     */
    public interface CEValueBuilder {

        CEValueBuilder withValue(String fieldName, Object value);

        Struct build();
    }

    /**
     * Adjust the name of CloudEvents attributes for Debezium events, following CloudEvents
     * <a href="https://github.com/cloudevents/spec/blob/v1.0/spec.md#attribute-naming-conventionattribute"> attribute
     * naming convention</a> as follows:
     *
     * <ul>
     * <li>prefixed with {@link #EXTENSION_NAME_PREFIX}</li>
     * <li>CloudEvents attribute names MUST consist of lower-case letters ('a' to 'z') or digits ('0' to '9') from the ASCII
     * character set, so any other characters are removed</li>
     * </ul>
     *
     * @param original the original field name
     * @return the valid extension attribute name
     */
    @VisibleForTesting
    static String adjustExtensionName(String original) {
        StringBuilder sb = new StringBuilder(EXTENSION_NAME_PREFIX);

        char c;
        for (int i = 0; i != original.length(); ++i) {
            c = original.charAt(i);
            if (isValidExtensionNameCharacter(c)) {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    private static boolean isValidExtensionNameCharacter(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
    }
}

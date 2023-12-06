/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters;

import static io.debezium.converters.spi.SerializerType.withName;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
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
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.CloudEventsConverterConfig.MetadataSource;
import io.debezium.converters.CloudEventsConverterConfig.MetadataSourceValue;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.recordandmetadata.RecordAndMetadataBaseImpl;
import io.debezium.converters.recordandmetadata.RecordAndMetadataHeaderImpl;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.CloudEventsValidator;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.SchemaNameAdjuster;

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
 * Configuration options of the underlying converters can be passed through using the {@code json} and {@code avro}
 * prefixes, respectively.
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
    private static final String TX_ATTRIBUTE_PREFIX = "tx";

    /**
     * Instantiated reflectively to avoid hard dependency to Avro converter.
     */
    private static final String CONFLUENT_AVRO_CONVERTER_CLASS = "io.confluent.connect.avro.AvroConverter";
    private static final String CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    private static String APICURIO_AVRO_CONVERTER_CLASS = "io.apicurio.registry.utils.converter.AvroConverter";
    private static final String APICURIO_SCHEMA_REGISTRY_URL_CONFIG = "apicurio.registry.url";

    /**
     * Suffix appended to schema names of data schema in case of Avro/Avro, to keep
     * data schema and CE schema apart from each other
     */
    private static final String DATA_SCHEMA_SUFFIX = "-data";

    private static final Logger LOGGER = LoggerFactory.getLogger(CloudEventsConverter.class);
    private static Method CONVERT_TO_CONNECT_METHOD;

    @Immutable
    private static Map<String, CloudEventsProvider> providers = new HashMap<>();

    static {
        try {
            // Use Kafka 3.5+ method signature
            CONVERT_TO_CONNECT_METHOD = JsonConverter.class.getDeclaredMethod("convertToConnect", Schema.class, JsonNode.class, JsonConverterConfig.class);
            CONVERT_TO_CONNECT_METHOD.setAccessible(true);
            LOGGER.info("Using up-to-date JsonConverter implementation");
        }
        catch (NoSuchMethodException e) {
            try {
                CONVERT_TO_CONNECT_METHOD = JsonConverter.class.getDeclaredMethod("convertToConnect", Schema.class, JsonNode.class);
                CONVERT_TO_CONNECT_METHOD.setAccessible(true);
                LOGGER.info("Using legacy JsonConverter implementation");
            }
            catch (NoSuchMethodException ei) {
                throw new DataException(ei);
            }
        }

        Map<String, CloudEventsProvider> tmp = new HashMap<>();

        for (CloudEventsProvider provider : ServiceLoader.load(CloudEventsProvider.class)) {
            tmp.put(provider.getName(), provider);
        }

        providers = Collections.unmodifiableMap(tmp);
    }

    private SerializerType ceSerializerType = withName(CloudEventsConverterConfig.CLOUDEVENTS_SERIALIZER_TYPE_DEFAULT);
    private SerializerType dataSerializerType = withName(CloudEventsConverterConfig.CLOUDEVENTS_DATA_SERIALIZER_TYPE_DEFAULT);

    private final JsonConverter jsonCloudEventsConverter = new JsonConverter();
    private JsonConverterConfig jsonCloudEventsConverterConfig = null;

    private JsonConverter jsonHeaderConverter = new JsonConverter();

    private final JsonConverter jsonDataConverter = new JsonConverter();

    private boolean enableJsonSchemas;
    private final JsonDeserializer jsonDeserializer = new JsonDeserializer();

    private Converter avroConverter;
    private List<String> schemaRegistryUrls;
    private SchemaNameAdjuster schemaNameAdjuster;

    private boolean extensionAttributesEnable;
    private String cloudEventsSchemaName;
    private MetadataSource metadataSource;

    private final CloudEventsValidator cloudEventsValidator = new CloudEventsValidator();

    public CloudEventsConverter() {
        this(null);
    }

    public CloudEventsConverter(Converter avroConverter) {
        this.avroConverter = avroConverter;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);

        Configuration jsonConfig = Configuration.from(configs).subset("json", true);

        conf.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        CloudEventsConverterConfig ceConfig = new CloudEventsConverterConfig(conf);
        ceSerializerType = ceConfig.cloudeventsSerializerType();
        dataSerializerType = ceConfig.cloudeventsDataSerializerTypeConfig();
        schemaNameAdjuster = ceConfig.schemaNameAdjustmentMode().createAdjuster();
        extensionAttributesEnable = ceConfig.extensionAttributesEnable();
        cloudEventsSchemaName = ceConfig.schemaCloudEventsName();
        metadataSource = ceConfig.metadataSource();

        Map<String, Object> jsonHeaderConverterConfig = new HashMap<>();
        jsonHeaderConverterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        jsonHeaderConverterConfig.put(JsonConverterConfig.TYPE_CONFIG, "header");
        jsonHeaderConverter.configure(jsonHeaderConverterConfig);

        boolean usingAvro = false;

        if (ceSerializerType == SerializerType.JSON) {
            Map<String, String> ceJsonConfig = jsonConfig.asMap();
            ceJsonConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
            configureConverterType(isKey, ceJsonConfig);
            jsonCloudEventsConverter.configure(ceJsonConfig, isKey);
            jsonCloudEventsConverterConfig = new JsonConverterConfig(ceJsonConfig);
        }
        else {
            usingAvro = true;

            if (dataSerializerType == SerializerType.JSON) {
                throw new IllegalStateException("Cannot use 'application/json' data content type within Avro events");
            }
        }

        if (dataSerializerType == SerializerType.JSON) {
            enableJsonSchemas = jsonConfig.getBoolean(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, JsonConverterConfig.SCHEMAS_ENABLE_DEFAULT);
            jsonDataConverter.configure(jsonConfig.asMap(), true);
        }
        else {
            usingAvro = true;
        }

        if (usingAvro) {
            Configuration avroConfig = Configuration.from(configs).subset("avro", true);

            boolean useApicurio = true;
            if (avroConfig.hasKey(APICURIO_SCHEMA_REGISTRY_URL_CONFIG)) {
                schemaRegistryUrls = avroConfig.getStrings(APICURIO_SCHEMA_REGISTRY_URL_CONFIG, ",");
            }
            else if (avroConfig.hasKey(CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG)) {
                schemaRegistryUrls = avroConfig.getStrings(CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG, ",");
                useApicurio = false;
            }

            if (schemaRegistryUrls == null || schemaRegistryUrls.isEmpty()) {
                throw new DataException("Need URL(s) for schema registry instances for CloudEvents when using Apache Avro");
            }

            if (avroConverter == null) {
                avroConverter = Instantiator.getInstance(useApicurio ? APICURIO_AVRO_CONVERTER_CLASS : CONFLUENT_AVRO_CONVERTER_CLASS);
                LOGGER.info("Using Avro converter {}", avroConverter.getClass().getName());
                avroConverter.configure(avroConfig.asMap(), false);
            }
        }

        cloudEventsValidator.configure(ceSerializerType, cloudEventsSchemaName);
    }

    protected Map<String, String> configureConverterType(boolean isKey, Map<String, String> config) {
        config.put("converter.type", isKey ? "key" : "value");
        return config;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return this.fromConnectData(topic, null, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        if (schema == null || value == null) {
            return null;
        }

        if (this.metadataSource.global() == MetadataSourceValue.VALUE) {
            if (!Envelope.isEnvelopeSchema(schema)) {
                // TODO Handling of non-data messages like schema change or transaction metadata
                return null;
            }
        }
        else {
            if (headers.lastHeader(Envelope.FieldName.SOURCE) == null || headers.lastHeader(Envelope.FieldName.OPERATION) == null) {
                return null;
            }
        }
        if (schema.type() != STRUCT) {
            throw new DataException("Mismatching schema");
        }

        Struct record = requireStruct(value, "CloudEvents converter");
        Struct source = getSource(record, headers);

        CloudEventsProvider provider = lookupCloudEventsProvider(source);

        RecordAndMetadata recordAndMetadata;
        boolean useBaseImpl = metadataSource.global() != MetadataSourceValue.HEADER && metadataSource.id() != MetadataSourceValue.HEADER
                && metadataSource.type() != MetadataSourceValue.HEADER;
        if (useBaseImpl) {
            recordAndMetadata = new RecordAndMetadataBaseImpl(record, schema);
        }
        else {
            recordAndMetadata = new RecordAndMetadataHeaderImpl(record, schema, headers, metadataSource, jsonHeaderConverter);
        }

        RecordParser parser = provider.createParser(recordAndMetadata);

        CloudEventsMaker maker = provider.createMaker(parser, dataSerializerType,
                (schemaRegistryUrls == null) ? null : String.join(",", schemaRegistryUrls), cloudEventsSchemaName);

        if (ceSerializerType == SerializerType.JSON) {
            if (dataSerializerType == SerializerType.JSON) {
                // JSON - JSON (with schema in data)
                if (enableJsonSchemas) {
                    SchemaBuilder dummy = SchemaBuilder.struct();
                    SchemaAndValue cloudEvent = convertToCloudEventsFormat(parser, maker, dummy, null, new Struct(dummy));

                    // need to create a JSON node with schema + payload first
                    byte[] data = jsonDataConverter.fromConnectData(topic, maker.ceDataAttributeSchema(), maker.ceDataAttribute());

                    // replace the dummy '{}' in '"data" : {}' with the schema + payload JSON node;
                    // the event itself must not have schema enabled, so to be a proper CloudEvent
                    byte[] cloudEventJson = jsonCloudEventsConverter.fromConnectData(topic, cloudEvent.schema(), cloudEvent.value());

                    ByteBuffer cloudEventWithData = ByteBuffer.allocate(cloudEventJson.length + data.length - 2);
                    cloudEventWithData.put(cloudEventJson, 0, cloudEventJson.length - 3);
                    cloudEventWithData.put(data);
                    cloudEventWithData.put((byte) '}');
                    return cloudEventWithData.array();
                }
                // JSON - JSON (without schema); can just use the regular JSON converter for the entire event
                else {
                    SchemaAndValue cloudEvent = convertToCloudEventsFormat(parser, maker, maker.ceDataAttributeSchema(), null, maker.ceDataAttribute());
                    return jsonCloudEventsConverter.fromConnectData(topic, cloudEvent.schema(), cloudEvent.value());
                }
            }
            // JSON - Avro; need to convert "data" to Avro first
            else {
                SchemaAndValue cloudEvent = convertToCloudEventsFormatWithDataAsAvro(topic, parser, maker);
                return jsonCloudEventsConverter.fromConnectData(topic, cloudEvent.schema(), cloudEvent.value());
            }
        }
        // Avro - Avro; need to convert "data" to Avro first
        else {
            SchemaAndValue cloudEvent = convertToCloudEventsFormatWithDataAsAvro(topic + DATA_SCHEMA_SUFFIX, parser, maker);
            return avroConverter.fromConnectData(topic, cloudEvent.schema(), cloudEvent.value());
        }
    }

    /**
     * Lookup the CloudEventsProvider implementation for the source connector.
     */
    private static CloudEventsProvider lookupCloudEventsProvider(Struct source) {
        String connectorType = source.getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
        CloudEventsProvider provider = providers.get(connectorType);
        if (provider != null) {
            return provider;
        }
        throw new DataException("No usable CloudEvents converters for connector type \"" + connectorType + "\"");
    }

    private Struct getSource(Struct record, Headers headers) {
        if (this.metadataSource.global() == MetadataSourceValue.VALUE) {
            return record.getStruct(Envelope.FieldName.SOURCE);
        }
        else {
            Header header = headers.lastHeader(Envelope.FieldName.SOURCE);
            SchemaAndValue sav = jsonHeaderConverter.toConnectData(null, header.value());
            return (Struct) sav.value();
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
                try {
                    // JSON Cloud Events converter always disables schema.
                    // The conversion back thus must be schemaless.
                    // If data are in schema/payload envelope they are extracted
                    final SchemaAndValue connectData = jsonCloudEventsConverter.toConnectData(topic, value);
                    cloudEventsValidator.verifyIsCloudEvent(connectData);

                    final JsonNode jsonValue = jsonDeserializer.deserialize(topic, value);
                    SchemaAndValue dataField = reconvertData(topic, jsonValue.get(CloudEventsMaker.FieldName.DATA), dataSerializerType, enableJsonSchemas);
                    ((Map<String, Object>) connectData.value()).put("data", dataField.value());

                    return connectData;
                }
                catch (SerializationException e) {
                    throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
                }
            case AVRO:
                // First reconvert the whole CloudEvents
                // Then reconvert the "data" field
                SchemaAndValue ceSchemaAndValue = avroConverter.toConnectData(topic, value);
                cloudEventsValidator.verifyIsCloudEvent(ceSchemaAndValue);
                Schema incompleteSchema = ceSchemaAndValue.schema();
                Struct ceValue = (Struct) ceSchemaAndValue.value();
                byte[] data = ceValue.getBytes(CloudEventsMaker.FieldName.DATA);
                SchemaAndValue dataSchemaAndValue = avroConverter.toConnectData(topic + DATA_SCHEMA_SUFFIX, data);
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

    private SchemaAndValue reconvertData(String topic, JsonNode data, SerializerType dataType, Boolean enableSchemas) {
        try {
            final byte[] serializedData = data.isBinary() ? data.binaryValue() : null;
            switch (dataType) {
                case JSON:
                    JsonNode jsonValue = data.isBinary() ? jsonDeserializer.deserialize(topic, serializedData) : data;

                    if (!enableSchemas) {
                        ObjectNode envelope = JsonNodeFactory.instance.objectNode();
                        envelope.set(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME, null);
                        envelope.set(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME, jsonValue);
                        jsonValue = envelope;
                    }

                    Schema schema = jsonCloudEventsConverter.asConnectSchema(jsonValue.get(CloudEventsMaker.FieldName.SCHEMA_FIELD_NAME));

                    try {
                        // Kafka 3.5+ requires additional argument
                        return new SchemaAndValue(schema,
                                (CONVERT_TO_CONNECT_METHOD.getParameterCount() == 2)
                                        ? CONVERT_TO_CONNECT_METHOD.invoke(jsonCloudEventsConverter, schema,
                                                jsonValue.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME))
                                        : CONVERT_TO_CONNECT_METHOD.invoke(jsonCloudEventsConverter, schema,
                                                jsonValue.get(CloudEventsMaker.FieldName.PAYLOAD_FIELD_NAME),
                                                jsonCloudEventsConverterConfig));
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
        catch (IOException e) {
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }
    }

    private SchemaAndValue convertToCloudEventsFormat(RecordParser parser, CloudEventsMaker maker, Schema dataSchemaType, String dataSchema, Object serializedData) {
        Struct source = parser.source();
        Schema sourceSchema = parser.source().schema();
        final Struct transaction = parser.transaction();

        // construct schema of CloudEvents envelope
        CESchemaBuilder ceSchemaBuilder = defineSchema()
                .withName(schemaNameAdjuster.adjust(maker.ceSchemaName()))
                .withSchema(CloudEventsMaker.FieldName.ID, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.SOURCE, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.SPECVERSION, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.TYPE, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.TIME, Schema.STRING_SCHEMA)
                .withSchema(CloudEventsMaker.FieldName.DATACONTENTTYPE, Schema.STRING_SCHEMA);

        if (dataSchema != null) {
            ceSchemaBuilder.withSchema(CloudEventsMaker.FieldName.DATASCHEMA, Schema.STRING_SCHEMA);
        }

        if (this.extensionAttributesEnable) {
            ceSchemaBuilder.withSchema(adjustExtensionName(Envelope.FieldName.OPERATION), Schema.STRING_SCHEMA);
            ceSchemaFromSchema(sourceSchema, ceSchemaBuilder, CloudEventsConverter::adjustExtensionName, false);
            // transaction attributes
            ceSchemaFromSchema(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA, ceSchemaBuilder, CloudEventsConverter::txExtensionName, true);
        }

        ceSchemaBuilder.withSchema(CloudEventsMaker.FieldName.DATA, dataSchemaType);

        Schema ceSchema = ceSchemaBuilder.build();

        String ceId = this.metadataSource.id() == MetadataSourceValue.GENERATE ? maker.ceId() : parser.id();
        String ceType = this.metadataSource.type() == MetadataSourceValue.GENERATE ? maker.ceType() : parser.type();

        // construct value of CloudEvents Envelope
        CEValueBuilder ceValueBuilder = withValue(ceSchema)
                .withValue(CloudEventsMaker.FieldName.ID, ceId)
                .withValue(CloudEventsMaker.FieldName.SOURCE, maker.ceSource(source.getString("name")))
                .withValue(CloudEventsMaker.FieldName.SPECVERSION, maker.ceSpecversion())
                .withValue(CloudEventsMaker.FieldName.TYPE, ceType)
                .withValue(CloudEventsMaker.FieldName.TIME, maker.ceTime())
                .withValue(CloudEventsMaker.FieldName.DATACONTENTTYPE, maker.ceDatacontenttype());

        if (dataSchema != null) {
            ceValueBuilder.withValue(CloudEventsMaker.FieldName.DATASCHEMA, dataSchema);
        }

        if (this.extensionAttributesEnable) {
            ceValueBuilder.withValue(adjustExtensionName(Envelope.FieldName.OPERATION), parser.op());
            ceValueFromStruct(source, sourceSchema, ceValueBuilder, CloudEventsConverter::adjustExtensionName);
            if (transaction != null) {
                ceValueFromStruct(transaction, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA, ceValueBuilder, CloudEventsConverter::txExtensionName);
            }
        }

        ceValueBuilder.withValue(CloudEventsMaker.FieldName.DATA, serializedData);

        return new SchemaAndValue(ceSchema, ceValueBuilder.build());
    }

    private void ceValueFromStruct(Struct struct, Schema schema, CEValueBuilder ceValueBuilder, Function<String, String> nameMapper) {
        for (Field field : schema.fields()) {
            Object value = struct.get(field);
            if (field.schema().type() == Type.INT64 && value != null) {
                value = String.valueOf((long) value);
            }
            ceValueBuilder.withValue(nameMapper.apply(field.name()), value);
        }
    }

    private void ceSchemaFromSchema(Schema schema, CESchemaBuilder ceSchemaBuilder, Function<String, String> nameMapper, boolean alwaysOptional) {
        for (Field field : schema.fields()) {
            ceSchemaBuilder.withSchema(nameMapper.apply(field.name()), convertToCeExtensionSchema(field.schema(), alwaysOptional));
        }
    }

    /**
     * Converts the given source attribute schema into a corresponding CE extension schema.
     * The types supported there are limited, e.g. int64 can only be represented as string.
     */
    private Schema convertToCeExtensionSchema(Schema schema, boolean alwaysOptional) {
        SchemaBuilder ceExtensionSchema;

        if (schema.type() == Type.BOOLEAN) {
            ceExtensionSchema = SchemaBuilder.bool();
        }
        // all numbers up to int32 go as int32
        else if (schema.type() == Type.INT8 || schema.type() == Type.INT16 || schema.type() == Type.INT16 || schema.type() == Type.INT32) {
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

        if (alwaysOptional || schema.isOptional()) {
            ceExtensionSchema.optional();
        }

        return ceExtensionSchema.build();
    }

    private Schema convertToCeExtensionSchema(Schema schema) {
        return convertToCeExtensionSchema(schema, false);
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

    private static String txExtensionName(String name) {
        return adjustExtensionName(TX_ATTRIBUTE_PREFIX + name);
    }

    private static boolean isValidExtensionNameCharacter(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
    }
}

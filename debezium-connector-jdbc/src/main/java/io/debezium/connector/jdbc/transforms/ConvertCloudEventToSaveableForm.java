/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsValidator;
import io.debezium.converters.spi.SerializerType;
import io.debezium.transforms.outbox.AdditionalFieldsValidator;

/**
 * The transform converts a CloudEvent to a structure suitable for `JdbcSinkConnector`
 *
 * @author Roman Kudryashov
 */
public class ConvertCloudEventToSaveableForm implements Transformation<SinkRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertCloudEventToSaveableForm.class);

    private static final String FIELD_NAME_SEPARATOR = ":";

    private static final Field FIELDS_MAPPING = Field.create("fields.mapping")
            .withDisplayName("Specifies a list of pairs with mappings between a CloudEvent's fields and names of database columns")
            .withType(ConfigDef.Type.LIST)
            .withValidation(AdditionalFieldsValidator::isListOfStringPairs)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Specifies a list of pairs with mappings between a CloudEvent's fields and names of database columns");

    private static final Field SERIALIZER_TYPE = Field.create("serializer.type")
            .withDisplayName("Specifies a serialization type a provided CloudEvent was serialized and deserialized with")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Specifies a serialization type a provided CloudEvent was serialized and deserialized with");

    private static final Field CLOUDEVENTS_SCHEMA_NAME = Field.create("schema.cloudevents.name")
            .withDisplayName("Specifies CloudEvents schema name under which the schema is registered in a Schema Registry")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Specifies CloudEvents schema name under which the schema is registered in a Schema Registry");

    private Map<String, String> fieldsMapping;

    private SerializerType serializerType;

    private String cloudEventsSchemaName;

    private final JsonConverter jsonDataConverter = new JsonConverter();

    private final Map<String, Schema> cloudEventsFieldToColumnSchema = new HashMap<>();

    private final CloudEventsValidator cloudEventsValidator = new CloudEventsValidator();

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, FIELDS_MAPPING, SERIALIZER_TYPE);
        return config;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);

        final List<String> rawFieldsMapping = config.getList(FIELDS_MAPPING);
        fieldsMapping = Collections.unmodifiableMap(parseFieldsMapping(rawFieldsMapping));

        serializerType = SerializerType.withName(config.getString(SERIALIZER_TYPE));
        if (serializerType == null) {
            throw new ConfigException(SERIALIZER_TYPE.name(), serializerType, "Serialization/deserialization type of CloudEvents converter is required");
        }

        cloudEventsSchemaName = config.getString(CLOUDEVENTS_SCHEMA_NAME);

        Map<String, Object> jsonDataConverterConfig = new HashMap<>();
        jsonDataConverterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        jsonDataConverterConfig.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonDataConverter.configure(jsonDataConverterConfig);

        cloudEventsFieldToColumnSchema.put(CloudEventsMaker.FieldName.ID, Schema.STRING_SCHEMA);
        cloudEventsFieldToColumnSchema.put(CloudEventsMaker.FieldName.SOURCE, Schema.STRING_SCHEMA);
        cloudEventsFieldToColumnSchema.put(CloudEventsMaker.FieldName.SPECVERSION, Schema.STRING_SCHEMA);
        cloudEventsFieldToColumnSchema.put(CloudEventsMaker.FieldName.TYPE, Schema.STRING_SCHEMA);
        cloudEventsFieldToColumnSchema.put(CloudEventsMaker.FieldName.DATACONTENTTYPE, Schema.STRING_SCHEMA);
        cloudEventsFieldToColumnSchema.put(CloudEventsMaker.FieldName.DATASCHEMA, Schema.STRING_SCHEMA);
        cloudEventsFieldToColumnSchema.put(CloudEventsMaker.FieldName.TIME, Schema.STRING_SCHEMA);
        cloudEventsFieldToColumnSchema.put(CloudEventsMaker.FieldName.DATA, Schema.STRING_SCHEMA);

        cloudEventsValidator.configure(serializerType, cloudEventsSchemaName);
    }

    private Map<String, String> parseFieldsMapping(List<String> rawFieldsMapping) {
        final Map<String, String> parsedFieldsMapping = new HashMap<>();
        for (String rawFieldMapping : rawFieldsMapping) {
            final String[] parts = rawFieldMapping.split(FIELD_NAME_SEPARATOR);
            final String cloudEventFieldName = parts[0];
            final String databaseColumnName;
            if (parts.length > 1) {
                databaseColumnName = parts[1];
            }
            else {
                databaseColumnName = cloudEventFieldName;
            }
            parsedFieldsMapping.put(cloudEventFieldName, databaseColumnName);
        }
        return parsedFieldsMapping;
    }

    @Override
    public SinkRecord apply(final SinkRecord record) {
        if (record == null || !cloudEventsValidator.isCloudEvent(new SchemaAndValue(record.valueSchema(), record.value())) || fieldsMapping.isEmpty()) {
            return record;
        }

        final boolean cloudEventContainsDataAsStruct;
        if (serializerType == SerializerType.JSON) {
            Object dataFieldValue = getCloudEventFieldsMap(record).get(CloudEventsMaker.FieldName.DATA);
            cloudEventContainsDataAsStruct = dataFieldValue instanceof Struct;
        }
        else {
            final org.apache.kafka.connect.data.Field dataField = record.valueSchema().field(CloudEventsMaker.FieldName.DATA);
            cloudEventContainsDataAsStruct = dataField != null && dataField.schema().type() == Schema.Type.STRUCT;
        }

        final Schema newSchema = getSchema(record, cloudEventContainsDataAsStruct);
        final Struct newValue = getValue(record, newSchema, cloudEventContainsDataAsStruct);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newValue,
                record.timestamp(),
                record.headers());
    }

    private Map<String, Object> getCloudEventFieldsMap(SinkRecord record) {
        return (Map<String, Object>) record.value();
    }

    private Schema getSchema(SinkRecord record, boolean cloudEventContainsDataAsStruct) {
        Map<String, Object> cloudEventMap = null;
        if (serializerType == SerializerType.JSON) {
            cloudEventMap = getCloudEventFieldsMap(record);
        }

        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        for (Map.Entry<String, String> fieldMapping : fieldsMapping.entrySet()) {
            final String cloudEventFieldName = fieldMapping.getKey();
            final String databaseColumnName = fieldMapping.getValue();

            final Schema cloudEventFieldSchema;
            if (serializerType == SerializerType.JSON) {
                Object cloudEventFieldValue = cloudEventMap.get(cloudEventFieldName);
                if (cloudEventFieldValue == null) {
                    // set default schemas
                    cloudEventFieldSchema = cloudEventsFieldToColumnSchema.get(cloudEventFieldName);
                }
                else {
                    cloudEventFieldSchema = determineCloudEventFieldSchema(cloudEventFieldValue);
                }
            }
            else {
                final org.apache.kafka.connect.data.Field cloudEventField = record.valueSchema().field(cloudEventFieldName);
                cloudEventFieldSchema = cloudEventField.schema();
            }

            final Schema databaseColumnSchema;
            if (cloudEventFieldName.equals(CloudEventsMaker.FieldName.DATA) && cloudEventContainsDataAsStruct) {
                databaseColumnSchema = Schema.STRING_SCHEMA;
            }
            else {
                databaseColumnSchema = cloudEventFieldSchema;
            }
            schemaBuilder.field(databaseColumnName, databaseColumnSchema);
        }
        return schemaBuilder.build();
    }

    private Schema determineCloudEventFieldSchema(Object cloudEventFieldValue) {
        final Schema cloudEventFieldSchema;
        if (cloudEventFieldValue instanceof String) {
            cloudEventFieldSchema = Schema.STRING_SCHEMA;
        }
        else if (cloudEventFieldValue instanceof Struct) {
            cloudEventFieldSchema = ((Struct) cloudEventFieldValue).schema();
        }
        else {
            throw new DataException("Unsupported type of CloudEvent field: " + cloudEventFieldValue.getClass());
        }
        return cloudEventFieldSchema;
    }

    private Struct getValue(SinkRecord record, Schema schema, boolean cloudEventContainsDataAsStruct) {
        Map<String, Object> cloudEventMap = null;
        Struct cloudEventStruct = null;
        if (serializerType == SerializerType.JSON) {
            cloudEventMap = getCloudEventFieldsMap(record);
        }
        else {
            cloudEventStruct = requireStruct(record.value(), "convert cloud event");
        }

        final Struct struct = new Struct(schema);
        for (Map.Entry<String, String> fieldMapping : fieldsMapping.entrySet()) {
            final String cloudEventFieldName = fieldMapping.getKey();
            final String databaseColumnName = fieldMapping.getValue();

            Object fieldValue;
            if (serializerType == SerializerType.JSON) {
                fieldValue = cloudEventMap.get(cloudEventFieldName);
            }
            else {
                fieldValue = cloudEventStruct.get(cloudEventFieldName);
            }
            if (cloudEventFieldName.equals(CloudEventsMaker.FieldName.DATA) && cloudEventContainsDataAsStruct) {
                final Struct data = (Struct) fieldValue;
                final byte[] dataInJson = jsonDataConverter.fromConnectData(null, data.schema(), data);
                fieldValue = new String(dataInJson);
            }

            struct.put(databaseColumnName, fieldValue);
        }
        return struct;
    }

    @Override
    public void close() {
    }
}

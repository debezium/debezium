/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transform;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.transforms.outbox.AdditionalFieldsValidator;

/**
 * The transform converts a CloudEvent to a structure suitable for `JdbcSinkConnector`. It uses provided by a user
 * mapping between a CloudEvent's fields and names of database columns. The resulting value schema has no name. A
 * CloudEvent's `data` field is flattened if needed
 *
 * @author Roman Kudryashov
 */
public class ConvertCloudEventToSaveableForm implements Transformation<SinkRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertCloudEventToSaveableForm.class);

    private static final String FIELD_NAME_SEPARATOR = ":";

    private static final String CLOUD_EVENTS_SCHEMA_NAME_SUFFIX = ".CloudEvents.Envelope";

    private static final Field FIELDS_MAPPING = Field.create("fields.mapping")
            .withDisplayName("Specifies a list of pairs with mappings between a CloudEvent's fields and names of database columns")
            .withType(ConfigDef.Type.LIST)
            .withValidation(AdditionalFieldsValidator::isListOfStringPairs)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Specifies a list of pairs with mappings between a CloudEvent's fields and names of database columns");

    private Map<String, String> fieldsMapping;

    private final JsonConverter jsonDataConverter = new JsonConverter();

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, FIELDS_MAPPING);
        return config;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);

        final List<String> rawFieldsMapping = config.getList(FIELDS_MAPPING);
        fieldsMapping = Collections.unmodifiableMap(parseFieldsMapping(rawFieldsMapping));

        Map<String, Object> jsonDataConverterConfig = new HashMap<>();
        jsonDataConverterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        jsonDataConverterConfig.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonDataConverter.configure(jsonDataConverterConfig);
    }

    private Map<String, String> parseFieldsMapping(List<String> rawFieldsMapping) {
        final Map<String, String> parsedFieldsMapping = new HashMap<>();
        for (String rawFieldMapping : rawFieldsMapping) {
            final String[] parts = rawFieldMapping.split(FIELD_NAME_SEPARATOR);
            final String cloudEventFieldName = parts[0];
            final String databaseColumnName;
            if (rawFieldMapping.contains(FIELD_NAME_SEPARATOR)) {
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
        if (!record.valueSchema().name().endsWith(CLOUD_EVENTS_SCHEMA_NAME_SUFFIX) || fieldsMapping.isEmpty()) {
            return null;
        }

        final org.apache.kafka.connect.data.Field dataField = record.valueSchema().field(CloudEventsMaker.FieldName.DATA);
        final boolean cloudEventContainsDataAsStruct = dataField != null && dataField.schema().type() == Schema.Type.STRUCT;

        final Schema newSchema = getSchema(record, cloudEventContainsDataAsStruct);
        final Struct newValue = getValue(record, newSchema, cloudEventContainsDataAsStruct);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newValue,
                record.timestamp());
    }

    private Schema getSchema(SinkRecord record, boolean cloudEventContainsDataAsStruct) {
        final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        for (Map.Entry<String, String> fieldMapping : fieldsMapping.entrySet()) {
            final String cloudEventFieldName = fieldMapping.getKey();
            final String databaseColumnName = fieldMapping.getValue();
            final org.apache.kafka.connect.data.Field cloudEventField = record.valueSchema().field(cloudEventFieldName);
            final Schema databaseColumnSchema;
            if (cloudEventFieldName.equals(CloudEventsMaker.FieldName.DATA) && cloudEventContainsDataAsStruct) {
                databaseColumnSchema = Schema.STRING_SCHEMA;
            }
            else {
                databaseColumnSchema = cloudEventField.schema();
            }
            schemaBuilder.field(databaseColumnName, databaseColumnSchema);
        }
        return schemaBuilder.build();
    }

    private Struct getValue(SinkRecord record, Schema schema, boolean cloudEventContainsDataAsStruct) {
        final Struct struct = new Struct(schema);
        final Struct cloudEvent = requireStruct(record.value(), "convert cloud event");
        for (Map.Entry<String, String> fieldMapping : fieldsMapping.entrySet()) {
            final String cloudEventFieldName = fieldMapping.getKey();
            final String databaseColumnName = fieldMapping.getValue();
            Object fieldValue = cloudEvent.get(cloudEventFieldName);
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

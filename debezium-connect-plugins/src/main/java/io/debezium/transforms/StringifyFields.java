/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.util.Strings;

/**
 * A Kafka Connect SMT that serializes selected record-value fields to JSON strings.
 * <p>
 * Some sinks accept a JSON <em>string</em> but reject a native struct, array or map for a
 * schema-flexible column, for example a Databricks Delta {@code VARIANT} column or a JSON/JSONB
 * column. After a document is flattened, such as by the MongoDB {@code ExtractNewDocumentState}
 * transformation, nested fields arrive as native structs and arrays, which those columns cannot
 * accept directly.
 * <p>
 * This transformation converts the configured fields into their JSON-string representation, so that
 * scalar fields remain strongly typed while a schema-flexible field is written as JSON text. Fields
 * that are already strings, or that are absent, are left untouched.
 * <p>
 * Configuration: {@code fields} &mdash; a comma-separated list of top-level value field names to
 * serialize.
 * <p>
 * If a targeted field cannot be serialized to JSON, the transformation logs the offending record's
 * context (field name, schema type and name, value class, and value) at {@code ERROR} level and
 * throws a {@link ConnectException}. The stream then halts and the offset is not committed past the
 * record, so that no data is dropped silently. To skip such records instead, set
 * {@code errors.tolerance=all}.
 *
 * @param <R> the type of {@link ConnectRecord} that the transformation applies to
 */
public class StringifyFields<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyFields.class);

    public static final String FIELDS_CONFIG = "fields";

    private static final Field FIELDS_FIELD = Field.create(FIELDS_CONFIG)
            .withDisplayName("Fields to serialize")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("Comma-separated list of value field names to serialize to a JSON string.");

    private static final ConfigDef CONFIG_DEF = new ConfigDef().define(
            FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
            FIELDS_FIELD.description());

    private Set<String> fields;
    private final JsonConverter jsonConverter = new JsonConverter();

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        final SmtManager<R> smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(FIELDS_FIELD));

        this.fields = new LinkedHashSet<>();
        for (String field : config.getList(FIELDS_FIELD)) {
            if (!Strings.isNullOrBlank(field)) {
                this.fields.add(field.trim());
            }
        }
        // schemas.enable=false emits plain JSON without the Connect schema envelope.
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", false);
        converterConfig.put("converter.type", "value");
        jsonConverter.configure(converterConfig);
    }

    @Override
    public R apply(R record) {
        if (!(record.value() instanceof Struct)) {
            return record;
        }
        Struct value = (Struct) record.value();
        Schema schema = value.schema();

        // Build a new schema in which the targeted fields become STRING.
        SchemaBuilder builder = SchemaBuilder.struct().name(schema.name()).version(schema.version());
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            if (fields.contains(field.name())) {
                builder.field(field.name(), field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
            }
            else {
                builder.field(field.name(), field.schema());
            }
        }
        Schema newSchema = builder.build();

        Struct newValue = new Struct(newSchema);
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            Object fieldValue = value.get(field);
            if (fields.contains(field.name()) && fieldValue != null && !(fieldValue instanceof String)) {
                newValue.put(field.name(), toJsonString(field.name(), field.schema(), fieldValue));
            }
            else {
                newValue.put(field.name(), fieldValue);
            }
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                newSchema, newValue, record.timestamp(), record.headers());
    }

    /**
     * Serializes a single field value to its JSON representation.
     */
    private String toJsonString(String fieldName, Schema fieldSchema, Object fieldValue) {
        try {
            byte[] json = jsonConverter.fromConnectData("_stringify", fieldSchema, fieldValue);
            return new String(json, java.nio.charset.StandardCharsets.UTF_8);
        }
        catch (Exception e) {
            // A field that cannot be serialized must halt the stream rather than be dropped or
            // corrupted silently, so log enough context to identify the offending record and then
            // re-throw, which leaves the offset uncommitted.
            String schemaType = fieldSchema == null ? "null" : String.valueOf(fieldSchema.type());
            String schemaName = fieldSchema == null ? "null" : String.valueOf(fieldSchema.name());
            String valueClass = fieldValue == null ? "null" : fieldValue.getClass().getName();
            LOGGER.error("Failed to serialize field '{}' (schema type={}, schema name={}, value class={}) to a JSON "
                    + "string. The record is not modified and the stream halts, so that no data is dropped or "
                    + "committed past this point. Value: {}",
                    fieldName, schemaType, schemaName, valueClass, fieldValue, e);
            throw new ConnectException("Could not serialize field '" + fieldName
                    + "' (schema type=" + schemaType + ", value class=" + valueClass + ") to a JSON string", e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(FIELDS_FIELD);
    }

    @Override
    public void close() {
        jsonConverter.close();
    }
}

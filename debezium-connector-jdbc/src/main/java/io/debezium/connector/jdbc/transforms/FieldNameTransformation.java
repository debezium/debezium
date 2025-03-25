/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.jdbc.Module;
import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.connector.jdbc.util.NamingStyleUtils;

/**
 * A Kafka Connect SMT (Single Message Transformation) that transforms field (column) names
 * in record values according to configured naming conventions.
 * <p>
 * This transformation can:
 * <ul>
 *   <li>Add a prefix to column names</li>
 *   <li>Add a suffix to column names</li>
 *   <li>Apply a naming style (e.g., snake_case, UPPER_CASE) to column names</li>
 * </ul>
 * <p>
 * The transformation preserves schema properties such as optionality and default values
 * while renaming the fields according to the configured conventions.
 *
 * @author Gustavo Lira
 * @param <R> The record type
 */
public class FieldNameTransformation<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldNameTransformation.class);

    // Configuration fields
    private static final String COLUMN_PREFIX_PARAM = "column.naming.prefix";
    private static final String COLUMN_SUFFIX_PARAM = "column.naming.suffix";
    private static final String COLUMN_STYLE_PARAM = "column.naming.style";

    private static final io.debezium.config.Field PREFIX = io.debezium.config.Field.create(COLUMN_PREFIX_PARAM)
            .withDisplayName("Column Name Prefix")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional prefix to add to column names.");

    private static final io.debezium.config.Field SUFFIX = io.debezium.config.Field.create(COLUMN_SUFFIX_PARAM)
            .withDisplayName("Column Name Suffix")
            .withType(ConfigDef.Type.STRING)
            .withDefault("")
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional suffix to add to column names.");

    private static final io.debezium.config.Field NAMING_STYLE = io.debezium.config.Field.create(COLUMN_STYLE_PARAM)
            .withDisplayName("Column Naming Style")
            .withType(ConfigDef.Type.STRING)
            .withDefault("default")
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The style of column naming: UPPERCASE, lowercase, snake_case, camelCase, kebab-case.");

    // Configuration state
    private String prefix;
    private String suffix;
    private NamingStyle namingStyle;

    /**
     * Configures this transformation with the given settings.
     *
     * @param configs the configuration properties
     */
    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        this.prefix = config.getString(PREFIX);
        this.suffix = config.getString(SUFFIX);
        this.namingStyle = NamingStyle.from(config.getString(NAMING_STYLE));

        LOGGER.info("Configured with prefix='{}', suffix='{}', naming style='{}'",
                prefix, suffix, namingStyle.getValue());
    }

    /**
     * Applies the field name transformation to the record.
     *
     * @param record the record to transform
     * @return a new record with transformed field names
     */
    @Override
    public R apply(final R record) {
        if (record.value() == null) {
            LOGGER.debug("Skipping null record value");
            return record;
        }

        if (!(record.value() instanceof Struct originalValue)) {
            LOGGER.debug("Skipping non-Struct record value of type: {}", record.value().getClass().getName());
            return record;
        }

        Schema originalSchema = record.valueSchema();

        if (originalSchema == null) {
            LOGGER.debug("Skipping record with null schema");
            return record;
        }

        try {
            // Build a new schema with transformed field names
            Schema transformedSchema = buildTransformedSchema(originalSchema);

            // Create a new struct with transformed field names
            Struct transformedValue = createTransformedStruct(originalValue, originalSchema, transformedSchema);

            // Create a new record with the transformed schema and value
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    transformedSchema,
                    transformedValue,
                    record.timestamp());
        }
        catch (Exception e) {
            LOGGER.error("Error transforming field names: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Builds a schema with transformed field names.
     *
     * @param originalSchema the original schema
     * @return a new schema with transformed field names
     */
    private Schema buildTransformedSchema(Schema originalSchema) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();

        // Copy schema-level properties
        copySchemaProperties(originalSchema, schemaBuilder);

        // Add fields with transformed names
        for (Field field : originalSchema.fields()) {
            String originalName = field.name();
            String transformedName = transformFieldName(originalName);

            SchemaBuilder fieldSchemaBuilder = copySchema(field.schema());
            schemaBuilder.field(transformedName, fieldSchemaBuilder.build());
        }

        return schemaBuilder.build();
    }

    /**
     * Creates a struct with transformed field names.
     *
     * @param originalValue the original struct value
     * @param originalSchema the original schema
     * @param transformedSchema the transformed schema
     * @return a new struct with transformed field names
     */
    private Struct createTransformedStruct(Struct originalValue, Schema originalSchema, Schema transformedSchema) {
        Struct transformedValue = new Struct(transformedSchema);

        for (Field field : originalSchema.fields()) {
            String originalName = field.name();
            String transformedName = transformFieldName(originalName);

            Object fieldValue = originalValue.get(originalName);
            if (fieldValue != null || field.schema().isOptional()) {
                transformedValue.put(transformedName, fieldValue);
            }
        }

        return transformedValue;
    }

    /**
     * Copies schema-level properties from the original schema to a schema builder.
     *
     * @param source the source schema
     * @param target the target schema builder
     */
    private void copySchemaProperties(Schema source, SchemaBuilder target) {
        if (source.name() != null) {
            target.name(source.name());
        }
        if (source.version() != null) {
            target.version(source.version());
        }
        if (source.doc() != null) {
            target.doc(source.doc());
        }
    }

    /**
     * Creates a schema builder that preserves the properties of the original schema.
     *
     * @param schema the schema to copy
     * @return a schema builder with copied properties
     */
    private SchemaBuilder copySchema(Schema schema) {
        SchemaBuilder builder;

        switch (schema.type()) {
            case BOOLEAN:
                builder = SchemaBuilder.bool();
                break;
            case INT8:
                builder = SchemaBuilder.int8();
                break;
            case INT16:
                builder = SchemaBuilder.int16();
                break;
            case INT32:
                builder = SchemaBuilder.int32();
                break;
            case INT64:
                builder = SchemaBuilder.int64();
                break;
            case FLOAT32:
                builder = SchemaBuilder.float32();
                break;
            case FLOAT64:
                builder = SchemaBuilder.float64();
                break;
            case STRING:
                builder = SchemaBuilder.string();
                break;
            case BYTES:
                builder = SchemaBuilder.bytes();
                break;
            case ARRAY:
                builder = SchemaBuilder.array(schema.valueSchema());
                break;
            case MAP:
                builder = SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
                break;
            case STRUCT:
                builder = SchemaBuilder.struct();
                for (Field field : schema.fields()) {
                    builder.field(field.name(), field.schema());
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported schema type: " + schema.type());
        }

        // Copy schema attributes
        if (schema.isOptional()) {
            builder.optional();
        }
        if (schema.defaultValue() != null) {
            builder.defaultValue(schema.defaultValue());
        }

        copySchemaProperties(schema, builder);

        return builder;
    }

    /**
     * Transforms a field name according to the configured naming style, prefix, and suffix.
     *
     * @param originalName the original field name
     * @return the transformed field name
     */
    private String transformFieldName(String originalName) {
        String transformedName = NamingStyleUtils.applyNamingStyle(originalName, namingStyle);
        return prefix + transformedName + suffix;
    }

    /**
     * Returns the configuration definition for this transformation.
     *
     * @return the configuration definition
     */
    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        io.debezium.config.Field.group(config, null, PREFIX, SUFFIX, NAMING_STYLE);
        return config;
    }

    /**
     * Closes this transformation and releases any resources.
     */
    @Override
    public void close() {
        // No resources to release
    }

    /**
     * Returns the version of this transformation.
     *
     * @return the version string
     */
    @Override
    public String version() {
        return Module.version();
    }
}
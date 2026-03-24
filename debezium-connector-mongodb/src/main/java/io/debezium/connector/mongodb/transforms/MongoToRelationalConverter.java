/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.transforms.Module;
import io.debezium.transforms.SmtManager;

/**
 * Converts MongoDB CDC events to relational-style format where 'before' and 'after'
 * fields are nested Struct objects instead of JSON strings. This enables MongoDB events
 * to be processed by relational SMTs like ExtractChangedRecordState.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Divyansh Agrawal
 */
public class MongoToRelationalConverter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoToRelationalConverter.class);

    // Configuration field for schema mapping
    private static final Field SCHEMA_MAPPING = Field.create("schema.mapping")
            .withDisplayName("Schema mapping configuration")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault("")
            .withDescription("JSON configuration for mapping MongoDB documents to a target schema. "
                    + "When provided, the SMT will normalize documents to match this schema, "
                    + "adding null values for missing fields.");

    // Configuration for handling missing fields
    private static final Field ADD_MISSING_FIELDS = Field.create("add.missing.fields")
            .withDisplayName("Add missing fields")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("When true and schema mapping is provided, missing fields will be "
                    + "added with null values to ensure schema consistency.");

    private final Field.Set configFields = Field.setOf(SCHEMA_MAPPING, ADD_MISSING_FIELDS);

    private Configuration config;
    private SmtManager<R> smtManager;
    private MongoDataConverter converter;
    private boolean addMissingFields;
    private String schemaMappingJson; 

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = Configuration.from(configs);
        this.smtManager = new SmtManager<>(config);

        addMissingFields = config.getBoolean(ADD_MISSING_FIELDS);
        schemaMappingJson = config.getString(SCHEMA_MAPPING);

        // Initialize the MongoDB data converter
        // Using ARRAY encoding and default field naming
        converter = new MongoDataConverter(ExtractNewDocumentState.ArrayEncoding.ARRAY);

        LOGGER.info("MongoToRelationalConverter configured with addMissingFields={}", addMissingFields);
    }

    @Override
    public R apply(R record) {
        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }

        // Return the record
        return record;
    }

    public Iterable<Field> validateConfigFields() {
        return configFields;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, configFields.asArray());
        return config;
    }
    
    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }
}

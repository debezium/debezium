/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.AbstractExtractNewRecordState;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.CONFIG_FIELDS;

/**
 * Converts MongoDB CDC events to relational-style format where 'before' and 'after'
 * fields are nested Struct objects instead of JSON strings. This enables MongoDB events
 * to be processed by relational SMTs like ExtractChangedRecordState.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Divyansh Agrawal
 */
public class MongoToRelationalConverter<R extends ConnectRecord<R>> extends AbstractExtractNewRecordState<R> {

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

    // Field set for configuration validation
    private final Field.Set configFields = CONFIG_FIELDS.with(SCHEMA_MAPPING, ADD_MISSING_FIELDS);

    // Instance variables - initialized in configure()
    private MongoDataConverter converter; // for converting BSON to Structs
    private boolean addMissingFields;
    private String schemaMappingJson; 

    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);

        // Get our custom configuration values
        addMissingFields = config.getBoolean(ADD_MISSING_FIELDS);
        schemaMappingJson = config.getString(SCHEMA_MAPPING);

        // Initialize the MongoDB data converter
        // Using ARRAY encoding and default field naming (similar to ExtractNewDocumentState)
        converter = new MongoDataConverter(ExtractNewDocumentState.ArrayEncoding.ARRAY);

        LOGGER.info("MongoToRelationalConverter configured with addMissingFields={}", addMissingFields);
    }

    

}

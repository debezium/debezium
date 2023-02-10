/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.mongodb.FieldSelector.FieldFilter;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.Json;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * @author Chris Cranford
 */
@ThreadSafe
public class MongoDbSchema implements DatabaseSchema<CollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSchema.class);

    // Change Streams schemas
    private static final String SCHEMA_NAME_UPDATED_DESCRIPTION = "io.debezium.connector.mongodb.changestream.updatedescription";
    private static final String SCHEMA_NAME_TRUNCATED_ARRAY = "io.debezium.connector.mongodb.changestream.truncatedarray";

    public static final Schema TRUNCATED_ARRAY_SCHEMA = SchemaBuilder.struct()
            .name(SCHEMA_NAME_TRUNCATED_ARRAY)
            .field(MongoDbFieldName.ARRAY_FIELD_NAME, Schema.STRING_SCHEMA)
            .field(MongoDbFieldName.ARRAY_NEW_SIZE, Schema.INT32_SCHEMA)
            .build();

    private final Filters filters;
    private final TopicSelector<CollectionId> topicSelector;
    private final Schema sourceSchema;
    private final SchemaNameAdjuster adjuster;
    private final ConcurrentMap<CollectionId, MongoDbCollectionSchema> collections = new ConcurrentHashMap<>();
    private final Serialization serialization;
    private final boolean enableBson;

    public MongoDbSchema(Filters filters, TopicSelector<CollectionId> topicSelector, Schema sourceSchema,
                         SchemaNameAdjuster schemaNameAdjuster, boolean enableBson) {
        this.filters = filters;
        this.topicSelector = topicSelector;
        this.sourceSchema = sourceSchema;
        this.adjuster = schemaNameAdjuster;
        this.enableBson = enableBson;
        this.serialization = enableBson ? new BsonSerialization() : new JsonSerialization();
    }

    public static Schema getUpdateDescriptionSchema(boolean isRaw) {
        Schema updatedFields = isRaw ? Schema.OPTIONAL_BYTES_SCHEMA : Json.builder().optional().build();
        return SchemaBuilder.struct()
                .optional()
                .name(SCHEMA_NAME_UPDATED_DESCRIPTION)
                .field(MongoDbFieldName.REMOVED_FIELDS,
                        SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .field(MongoDbFieldName.UPDATED_FIELDS, updatedFields)
                .field(MongoDbFieldName.TRUNCATED_ARRAYS,
                        SchemaBuilder.array(TRUNCATED_ARRAY_SCHEMA).optional().build())
                .build();
    }

    @Override
    public void close() {
    }

    @Override
    public DataCollectionSchema schemaFor(CollectionId collectionId) {
        return collections.computeIfAbsent(collectionId, id -> {
            final FieldFilter fieldFilter = filters.fieldFilterFor(id);
            final String topicName = topicSelector.topicNameFor(id);

            final Schema keySchema = SchemaBuilder.struct()
                    .name(adjuster.adjust(topicName + ".Key"))
                    .field("id", Schema.STRING_SCHEMA)
                    .build();

            Schema beforeSchema;
            Schema afterSchema;
            if (enableBson) {
                beforeSchema = Schema.OPTIONAL_BYTES_SCHEMA;
                afterSchema = Schema.OPTIONAL_BYTES_SCHEMA;
            }
            else {
                beforeSchema = Json.builder().optional().build();
                afterSchema = Json.builder().optional().build();
            }

            final Schema valueSchema = SchemaBuilder.struct()
                    .name(adjuster.adjust(Envelope.schemaName(topicName)))
                    .field(FieldName.BEFORE, beforeSchema)
                    .field(FieldName.AFTER, afterSchema)
                    // Oplog fields
                    .field(MongoDbFieldName.PATCH, Json.builder().optional().build())
                    .field(MongoDbFieldName.FILTER, Json.builder().optional().build())
                    // Change Streams field
                    .field(MongoDbFieldName.UPDATE_DESCRIPTION, getUpdateDescriptionSchema(enableBson))
                    .field(FieldName.SOURCE, sourceSchema)
                    .field(FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                    .field(FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                    // Extra field for raw oplogs
                    .field(MongoDbFieldName.RAW_OPLOG_FIELD, Schema.OPTIONAL_BYTES_SCHEMA)
                    .field(MongoDbFieldName.STRIPE_AUDIT, Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

            final Envelope envelope = Envelope.fromSchema(valueSchema);

            return new MongoDbCollectionSchema(
                    id,
                    fieldFilter,
                    keySchema,
                    serialization::getDocumentIdOplog,
                    serialization::getDocumentIdChangeStream,
                    envelope,
                    valueSchema,
                    serialization::getDocumentValue);
        });
    }

    @Override
    public boolean tableInformationComplete() {
        // Mongo does not support HistonizedDatabaseSchema - so no tables are recovered
        return false;
    }

    @Override
    public void assureNonEmptySchema() {
        if (collections.isEmpty()) {
            LOGGER.warn(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING);
        }
    }

    @Override
    public boolean isHistorized() {
        return false;
    }
}

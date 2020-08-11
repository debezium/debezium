/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.bson.Document;
import org.bson.codecs.Encoder;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.util.JSONSerializers;
import com.mongodb.util.ObjectSerializer;

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
public class MongoDbSchema implements DatabaseSchema<CollectionId> {

    /**
     * Common settings for writing JSON strings using a compact JSON format
     */
    public static final JsonWriterSettings COMPACT_JSON_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.STRICT)
            .indent(true)
            .indentCharacters("")
            .newLineCharacters("")
            .build();

    private static final ObjectSerializer jsonSerializer = JSONSerializers.getStrict();
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSchema.class);

    private final Filters filters;
    private final TopicSelector<CollectionId> topicSelector;
    private final Schema sourceSchema;
    private final SchemaNameAdjuster adjuster = SchemaNameAdjuster.create(LOGGER);
    private final Function<Document, String> valueTransformer;
    private final Map<CollectionId, MongoDbCollectionSchema> collections = new HashMap<>();

    public MongoDbSchema(Filters filters, TopicSelector<CollectionId> topicSelector, Schema sourceSchema) {
        this.filters = filters;
        this.topicSelector = topicSelector;
        this.sourceSchema = sourceSchema;
        this.valueTransformer = resolveValueTransformer();
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

            final Schema valueSchema = SchemaBuilder.struct()
                    .name(adjuster.adjust(Envelope.schemaName(topicName)))
                    .field(FieldName.AFTER, Json.builder().optional().build())
                    .field("patch", Json.builder().optional().build())
                    .field("filter", Json.builder().optional().build())
                    .field(FieldName.SOURCE, sourceSchema)
                    .field(FieldName.OPERATION, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                    .field(FieldName.TRANSACTION, TransactionMonitor.TRANSACTION_BLOCK_SCHEMA)
                    .build();

            final Envelope envelope = Envelope.fromSchema(valueSchema);

            return new MongoDbCollectionSchema(
                    id,
                    fieldFilter,
                    keySchema,
                    this::getDocumentId,
                    envelope,
                    valueSchema,
                    this::getDocumentValue);
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

    private String getDocumentId(Document document) {
        if (document == null) {
            return null;
        }
        return jsonSerializer.serialize(document.get(DBCollection.ID_FIELD_NAME));
    }

    private String getDocumentValue(Document document) {
        return valueTransformer.apply(document);
    }

    private static Function<Document, String> resolveValueTransformer() {
        Encoder<Document> encoder = MongoClient.getDefaultCodecRegistry().get(Document.class);
        return (doc) -> doc.toJson(COMPACT_JSON_SETTINGS, encoder);
    }
}

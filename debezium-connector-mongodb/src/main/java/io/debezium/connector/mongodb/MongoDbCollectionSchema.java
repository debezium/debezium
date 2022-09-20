/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.TruncatedArray;

import io.debezium.connector.mongodb.FieldSelector.FieldFilter;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.SchemaUtil;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Defines the Kafka Connect {@link Schema} functionality associated with a given mongodb collection, and which can
 * be used to send documents that match the schema to Kafka Connect.
 *
 * @author Chris Cranford
 */
public class MongoDbCollectionSchema implements DataCollectionSchema {

    private final CollectionId id;
    private final FieldFilter fieldFilter;
    private final Schema keySchema;
    private final Envelope envelopeSchema;
    private final Schema valueSchema;
    private final Function<BsonDocument, Object> keyGeneratorOplog;
    private final Function<BsonDocument, Object> keyGeneratorChangeStream;
    private final Function<BsonDocument, String> valueGenerator;

    public MongoDbCollectionSchema(CollectionId id, FieldFilter fieldFilter, Schema keySchema, Function<BsonDocument, Object> keyGenerator,
                                   Function<BsonDocument, Object> keyGeneratorChangeStream, Envelope envelopeSchema, Schema valueSchema,
                                   Function<BsonDocument, String> valueGenerator) {
        this.id = id;
        this.fieldFilter = fieldFilter;
        this.keySchema = keySchema;
        this.envelopeSchema = envelopeSchema;
        this.valueSchema = valueSchema;
        this.keyGeneratorOplog = keyGenerator != null ? keyGenerator : (Document) -> null;
        this.keyGeneratorChangeStream = keyGeneratorChangeStream != null ? keyGeneratorChangeStream : (BsonDocument) -> null;
        this.valueGenerator = valueGenerator != null ? valueGenerator : (Document) -> null;
    }

    @Override
    public DataCollectionId id() {
        return id;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    @Override
    public Envelope getEnvelopeSchema() {
        return envelopeSchema;
    }

    public Struct keyFromDocumentOplog(BsonDocument document) {
        return document == null ? null : new Struct(keySchema).put("id", keyGeneratorOplog.apply(document));
    }

    public Struct keyFromDocument(BsonDocument document) {
        return document == null ? null : new Struct(keySchema).put("id", keyGeneratorChangeStream.apply(document));
    }

    public Struct valueFromDocumentOplog(BsonDocument document, BsonDocument filter, Envelope.Operation operation) {
        Struct value = new Struct(valueSchema);
        switch (operation) {
            case READ:
            case CREATE:
                final String jsonStr = valueGenerator.apply(fieldFilter.apply(document));
                value.put(FieldName.AFTER, jsonStr);
                break;
            case UPDATE:
                final String patchStr = valueGenerator.apply(fieldFilter.apply(document));
                value.put(MongoDbFieldName.PATCH, patchStr);
                final String updateFilterStr = valueGenerator.apply(fieldFilter.apply(filter));
                value.put(MongoDbFieldName.FILTER, updateFilterStr);
                break;
            case DELETE:
                final String deleteFilterStr = valueGenerator.apply(fieldFilter.apply(filter));
                value.put(MongoDbFieldName.FILTER, deleteFilterStr);
                break;
        }
        return value;
    }

    public Struct valueFromDocumentChangeStream(ChangeStreamDocument<BsonDocument> document, Envelope.Operation operation) {
        Struct value = new Struct(valueSchema);
        switch (operation) {
            case CREATE:
                extractFullDocument(document, value);
                break;
            case UPDATE:
                // Not null when full documents before change are enabled
                if (document.getFullDocumentBeforeChange() != null) {
                    extractFullDocumentBeforeChange(document, value);
                }

                // Not null when full documents are enabled for updates
                if (document.getFullDocument() != null) {
                    extractFullDocument(document, value);
                }

                if (document.getUpdateDescription() != null) {
                    final Struct updateDescription = new Struct(MongoDbSchema.UPDATED_DESCRIPTION_SCHEMA);
                    List<String> removedFields = document.getUpdateDescription().getRemovedFields();
                    if (removedFields != null && !removedFields.isEmpty()) {
                        removedFields = removedFields.stream()
                                .map(x -> fieldFilter.apply(x))
                                .filter(x -> x != null)
                                .collect(Collectors.toList());
                        if (!removedFields.isEmpty()) {
                            updateDescription.put(MongoDbFieldName.REMOVED_FIELDS, removedFields);
                        }
                    }

                    final BsonDocument updatedFields = document.getUpdateDescription().getUpdatedFields();
                    if (updatedFields != null) {
                        updateDescription.put(MongoDbFieldName.UPDATED_FIELDS, fieldFilter.applyChange(updatedFields).toJson());
                    }

                    // TODO Test filters for truncated arrays
                    List<TruncatedArray> truncatedArrays = document.getUpdateDescription().getTruncatedArrays();
                    if (truncatedArrays != null && !truncatedArrays.isEmpty()) {
                        truncatedArrays = truncatedArrays.stream()
                                .map(x -> new TruncatedArray(fieldFilter.apply(x.getField()), x.getNewSize()))
                                .filter(x -> x.getField() != null)
                                .collect(Collectors.toList());
                        if (!truncatedArrays.isEmpty()) {
                            updateDescription.put(MongoDbFieldName.TRUNCATED_ARRAYS, truncatedArrays.stream().map(x -> {
                                final Struct element = new Struct(MongoDbSchema.TRUNCATED_ARRAY_SCHEMA);
                                element.put(MongoDbFieldName.ARRAY_FIELD_NAME, x.getField());
                                element.put(MongoDbFieldName.ARRAY_NEW_SIZE, x.getNewSize());
                                return element;
                            }).collect(Collectors.toList()));
                        }
                    }

                    value.put(MongoDbFieldName.UPDATE_DESCRIPTION, updateDescription);
                }
                break;
            case DELETE:
                // Not null when full documents before change are enabled
                if (document.getFullDocumentBeforeChange() != null) {
                    extractFullDocumentBeforeChange(document, value);
                }
                break;
        }
        return value;
    }

    private void extractFullDocument(ChangeStreamDocument<BsonDocument> document, Struct value) {
        final String fullDocStr = valueGenerator.apply(fieldFilter.apply(document.getFullDocument()));
        value.put(FieldName.AFTER, fullDocStr);
    }

    private void extractFullDocumentBeforeChange(ChangeStreamDocument<BsonDocument> document, Struct value) {
        final String fullDocBeforeChangeStr = valueGenerator.apply(fieldFilter.apply(document.getFullDocumentBeforeChange()));
        value.put(FieldName.BEFORE, fullDocBeforeChangeStr);
    }

    @Override
    public int hashCode() {
        return valueSchema().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof MongoDbCollectionSchema) {
            MongoDbCollectionSchema that = (MongoDbCollectionSchema) obj;
            return Objects.equals(this.keySchema(), that.keySchema()) && Objects.equals(this.valueSchema(), that.valueSchema());
        }
        return false;
    }

    @Override
    public String toString() {
        return "{ key : " + SchemaUtil.asString(keySchema()) + ", value : " + SchemaUtil.asString(valueSchema()) + " }";
    }
}

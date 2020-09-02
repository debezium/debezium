/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.bson.Document;

import io.debezium.connector.mongodb.FieldSelector.FieldFilter;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.FieldName;
import io.debezium.data.SchemaUtil;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;

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
    private final Envelope enveopeSchema;
    private final Schema valueSchema;
    private final Function<Document, Object> keyGenerator;
    private final Function<Document, String> valueGenerator;

    public MongoDbCollectionSchema(CollectionId id, FieldFilter fieldFilter, Schema keySchema, Function<Document, Object> keyGenerator,
                                   Envelope envelopeSchema, Schema valueSchema, Function<Document, String> valueGenerator) {
        this.id = id;
        this.fieldFilter = fieldFilter;
        this.keySchema = keySchema;
        this.enveopeSchema = envelopeSchema;
        this.valueSchema = valueSchema;
        this.keyGenerator = keyGenerator != null ? keyGenerator : (Document) -> null;
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
        return enveopeSchema;
    }

    public Struct keyFromDocument(Document document) {
        return document == null ? null : new Struct(keySchema).put("id", keyGenerator.apply(document));
    }

    public Struct valueFromDocument(Document document, Document filter, Envelope.Operation operation) {
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

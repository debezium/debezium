/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.mongodb;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.eventhandler.EventOperation;
import io.debezium.data.Envelope;
import io.debezium.sink.naming.ColumnNamingStrategy;

/**
 * An {@link EventOperation} implementation for Debezium MongoDB source connector insert events.
 */
public class MongoDbInsertEvent implements EventOperation {

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc, ColumnNamingStrategy columnNamingStrategy) {
        BsonDocument valueDoc = doc.getValueDoc()
                .orElseThrow(() -> new DataException("Value document is missing for insert operation"));

        if (!valueDoc.containsKey(Envelope.FieldName.AFTER) || !valueDoc.get(Envelope.FieldName.AFTER).isString()) {
            throw new DataException("Value document is missing required 'after' string field for insert operation");
        }

        String afterStr = valueDoc.get(Envelope.FieldName.AFTER).asString().getValue();
        BsonDocument afterDoc;
        try {
            afterDoc = BsonDocument.parse(afterStr);
        }
        catch (RuntimeException e) {
            throw new DataException("Failed to parse '" + Envelope.FieldName.AFTER + "' field as JSON for insert operation", e);
        }

        BsonDocument keyDoc = doc.getKeyDoc()
                .orElseThrow(() -> new DataException("Key document is missing for insert operation"));

        BsonDocument filterDoc = MongoDbEventHandler.generateFilterDoc(keyDoc);

        return new ReplaceOneModel<>(filterDoc, afterDoc, new ReplaceOptions().upsert(true));
    }
}

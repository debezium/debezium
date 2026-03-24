/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.mongodb;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.eventhandler.EventOperation;
import io.debezium.sink.naming.ColumnNamingStrategy;

/**
 * An {@link EventOperation} implementation for Debezium MongoDB source connector delete events.
 */
public class MongoDbDeleteEvent implements EventOperation {

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc, ColumnNamingStrategy columnNamingStrategy) {
        BsonDocument keyDoc = doc.getKeyDoc()
                .orElseThrow(() -> new DataException("Key document is missing for delete operation"));

        BsonDocument filterDoc = MongoDbEventHandler.generateFilterDoc(keyDoc);

        return new DeleteOneModel<>(filterDoc);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.relational;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.eventhandler.EventOperation;
import io.debezium.data.Envelope.Operation;
import io.debezium.table.ColumnNamingStrategy;

public class RelationalDeleteEvent implements EventOperation {

    @Override
    public WriteModel<BsonDocument> perform(final SinkDocument doc, ColumnNamingStrategy columnNamingStrategy) {

        BsonDocument keyDoc = doc.getKeyDoc()
                .orElseThrow(
                        () -> new DataException("Key document must not be missing for delete operation"));

        BsonDocument valueDoc = doc.getValueDoc()
                .orElseThrow(
                        () -> new DataException("Value document must not be missing for delete operation"));

        try {
            BsonDocument filterDoc = RelationalEventHandler.generateFilterDoc(keyDoc, valueDoc, Operation.DELETE, columnNamingStrategy);
            return new DeleteOneModel<>(filterDoc);
        }
        catch (Exception exc) {
            throw new DataException(exc);
        }
    }
}

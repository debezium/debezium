/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.relational;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.connector.mongodb.sink.eventhandler.EventOperation;
import io.debezium.data.Envelope.Operation;
import io.debezium.sink.naming.ColumnNamingStrategy;

public class RelationalInsertEvent implements EventOperation {

    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> perform(final SinkDocument doc, ColumnNamingStrategy columnNamingStrategy) {

        BsonDocument keyDoc = doc.getKeyDoc()
                .orElseThrow(
                        () -> new DataException("Key document must not be missing for insert operation"));

        BsonDocument valueDoc = doc.getValueDoc()
                .orElseThrow(
                        () -> new DataException("Value document must not be missing for insert operation"));

        try {
            BsonDocument filterDoc = RelationalEventHandler.generateFilterDoc(keyDoc, valueDoc, Operation.CREATE, columnNamingStrategy);
            BsonDocument upsertDoc = RelationalEventHandler.generateUpsertOrReplaceDoc(keyDoc, valueDoc, filterDoc, columnNamingStrategy);
            return new ReplaceOneModel<>(filterDoc, upsertDoc, REPLACE_OPTIONS);
        }
        catch (Exception exc) {
            throw new DataException(exc);
        }
    }
}

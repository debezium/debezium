/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig.ID_FIELD;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;

public class DeleteDefaultStrategy implements WriteModelStrategy {
    private final IdStrategy idStrategy;

    public DeleteDefaultStrategy() {
        this(new DefaultIdFieldStrategy());
    }

    public DeleteDefaultStrategy(final IdStrategy idStrategy) {
        this.idStrategy = idStrategy;
    }

    public IdStrategy getIdStrategy() {
        return this.idStrategy;
    }

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
        document.getKeyDoc()
                .orElseThrow(() -> new DataException("Could not build the WriteModel,the key document was missing unexpectedly"));

        BsonDocument deleteFilter;
        if (idStrategy instanceof DefaultIdFieldStrategy) {
            deleteFilter = idStrategy.generateId(document, null).asDocument();
        }
        else {
            deleteFilter = new BsonDocument(ID_FIELD, idStrategy.generateId(document, null));
        }
        return new DeleteOneModel<>(deleteFilter);
    }

    static class DefaultIdFieldStrategy implements IdStrategy {
        @Override
        public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
            BsonDocument kd = doc.getKeyDoc().get();
            return kd.containsKey(ID_FIELD) ? kd : new BsonDocument(ID_FIELD, kd);
        }
    }
}

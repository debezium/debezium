/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import static io.debezium.connector.mongodb.sink.MongoDbSinkConnectorConfig.ID_FIELD;

import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;

public class ReplaceDefaultStrategy implements WriteModelStrategy {

    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
        BsonDocument vd = document
                .getValueDoc()
                .orElseThrow(
                        () -> new DataException(
                                "Could not build the WriteModel,the value document was missing unexpectedly"));

        BsonValue idValue = vd.get(ID_FIELD);
        if (idValue == null) {
            throw new DataException(
                    "Could not build the WriteModel,the `_id` field was missing unexpectedly");
        }

        return new ReplaceOneModel<>(new BsonDocument(ID_FIELD, idValue), vd, REPLACE_OPTIONS);
    }
}

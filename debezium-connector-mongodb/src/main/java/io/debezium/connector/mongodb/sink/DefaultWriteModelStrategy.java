/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;

public class DefaultWriteModelStrategy implements WriteModelStrategy {

    private final WriteModelStrategy writeModelStrategy = new ReplaceDefaultStrategy();

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
        return writeModelStrategy.createWriteModel(document);
    }

    WriteModelStrategy getWriteModelStrategy() {
        return writeModelStrategy;
    }
}

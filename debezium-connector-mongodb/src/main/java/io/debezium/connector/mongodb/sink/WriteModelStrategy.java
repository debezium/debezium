/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;

public interface WriteModelStrategy {

    WriteModel<BsonDocument> createWriteModel(SinkDocument document);
}

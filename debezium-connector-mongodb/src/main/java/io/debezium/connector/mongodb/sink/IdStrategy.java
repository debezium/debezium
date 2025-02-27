/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonValue;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;

/**
 * Strategy that returns the identity of the MongoDB document. This is typically the _id field of the document.
 */
public interface IdStrategy {

    BsonValue generateId(SinkDocument doc, SinkRecord orig);
}

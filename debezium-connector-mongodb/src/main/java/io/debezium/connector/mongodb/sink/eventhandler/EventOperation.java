/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import io.debezium.connector.mongodb.sink.converters.SinkDocument;
import io.debezium.table.ColumnNamingStrategy;

public interface EventOperation {

    WriteModel<BsonDocument> perform(SinkDocument doc, ColumnNamingStrategy columnNamingStrategy);
}

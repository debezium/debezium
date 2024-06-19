/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDocument;

/**
 * Interface to convert a value (from a Kafka Connect record) into a BsonDocument representing the value.
 */
public interface SinkValueConverter {

    BsonDocument convert(Schema schema, Object value);
}

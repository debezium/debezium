/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.bson.BsonDocument;

public interface Serialization {

    Object getDocumentIdOplog(BsonDocument document);

    Object getDocumentIdChangeStream(BsonDocument document);

    Object getDocumentValue(BsonDocument document);
}

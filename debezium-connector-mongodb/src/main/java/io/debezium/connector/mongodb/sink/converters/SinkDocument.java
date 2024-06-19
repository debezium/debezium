/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import java.util.Optional;

import org.bson.BsonDocument;

public final class SinkDocument implements Cloneable {
    private final BsonDocument keyDoc;
    private final BsonDocument valueDoc;

    public SinkDocument(final BsonDocument keyDoc, final BsonDocument valueDoc) {
        this.keyDoc = keyDoc;
        this.valueDoc = valueDoc;
    }

    public Optional<BsonDocument> getKeyDoc() {
        return Optional.ofNullable(keyDoc);
    }

    public Optional<BsonDocument> getValueDoc() {
        return Optional.ofNullable(valueDoc);
    }

    @Override
    public SinkDocument clone() {
        return new SinkDocument(keyDoc != null ? keyDoc.clone() : null, valueDoc != null ? valueDoc.clone() : null);
    }
}

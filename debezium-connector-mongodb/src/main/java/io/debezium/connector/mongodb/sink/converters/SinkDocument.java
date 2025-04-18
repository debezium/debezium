/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.converters;

import java.util.Optional;

import org.bson.BsonDocument;

public final class SinkDocument implements Cloneable {
    private final Optional<BsonDocument> keyDoc;
    private final Optional<BsonDocument> valueDoc;

    public SinkDocument(final BsonDocument keyDoc, final BsonDocument valueDoc) {
        this.keyDoc = Optional.ofNullable(keyDoc);
        this.valueDoc = Optional.ofNullable(valueDoc);
    }

    public Optional<BsonDocument> getKeyDoc() {
        return keyDoc;
    }

    public Optional<BsonDocument> getValueDoc() {
        return valueDoc;
    }

    @Override
    public SinkDocument clone() {
        return new SinkDocument(keyDoc.isPresent() ? keyDoc.get().clone() : null,
                valueDoc.isPresent() ? valueDoc.get().clone() : null);
    }
}

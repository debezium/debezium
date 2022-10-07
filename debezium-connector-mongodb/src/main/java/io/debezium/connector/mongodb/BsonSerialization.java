/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

public class BsonSerialization implements Serialization {
    private final static JsonSerialization jsonSerialization = new JsonSerialization();

    @Override
    public Object getDocumentIdOplog(BsonDocument document) {
        return jsonSerialization.getDocumentIdOplog(document);
    }

    @Override
    public Object getDocumentIdChangeStream(BsonDocument document) {
        return jsonSerialization.getDocumentIdChangeStream(document);
    }

    @Override
    public Object getDocumentValue(BsonDocument document) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        (new BsonDocumentCodec()).encode(writer, document, EncoderContext.builder().build());
        return outputBuffer.toByteArray();
    }
}

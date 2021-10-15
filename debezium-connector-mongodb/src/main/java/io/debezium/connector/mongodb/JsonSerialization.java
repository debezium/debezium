/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.function.Function;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.Encoder;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;

/**
 * A class responsible for serialization of message keys and values to MongoDB compatible JSON
 *
 * @author Jiri Pechanec
 *
 */
class JsonSerialization {

    @FunctionalInterface
    public static interface Transformer extends Function<Document, String> {
    }

    private static final String ID_FIELD_NAME = "_id";

    /**
     * Common settings for writing JSON strings using a compact JSON format
     */
    public static final JsonWriterSettings COMPACT_JSON_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.STRICT)
            .indent(true)
            .indentCharacters("")
            .newLineCharacters("")
            .build();

    /**
     * Common settings for writing JSON strings using a compact JSON format
     */
    private static final JsonWriterSettings SIMPLE_JSON_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .indent(true)
            .indentCharacters("")
            .newLineCharacters("")
            .build();

    private final Transformer transformer;

    public JsonSerialization() {
        final Encoder<Document> encoder = MongoClientSettings.getDefaultCodecRegistry().get(Document.class);
        transformer = (doc) -> doc.toJson(COMPACT_JSON_SETTINGS, encoder);
    }

    public String getDocumentIdOplog(Document document) {
        if (document == null) {
            return null;
        }
        // The serialized value is in format {"_": xxx} so we need to remove the starting dummy field name and closing brace
        final String keyValue = new BasicDBObject("_", document.get(ID_FIELD_NAME)).toJson(SIMPLE_JSON_SETTINGS);
        final int start = 6;
        final int end = keyValue.length() - 1;
        if (!(end > start)) {
            throw new IllegalStateException("Serialized JSON object '" + keyValue + "' is not in expected format");
        }
        return keyValue.substring(start, end);
    }

    public String getDocumentIdChangeStream(BsonDocument document) {
        if (document == null) {
            return null;
        }
        // The serialized value is in format {"_": xxx} so we need to remove the starting dummy field name and closing brace
        final String keyValue = document.toJson(SIMPLE_JSON_SETTINGS);
        final int start = 8;
        final int end = keyValue.length() - 1;
        if (!(end > start)) {
            throw new IllegalStateException("Serialized JSON object '" + keyValue + "' is not in expected format");
        }
        return keyValue.substring(start, end);
    }

    public String getDocumentValue(Document document) {
        return transformer.apply(document);
    }

    public Transformer getTransformer() {
        return transformer;
    }
}

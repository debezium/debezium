/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.events;

import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;

import io.debezium.connector.mongodb.events.BufferingChangeStreamCursor.ResumableChangeStreamEvent;

/**
 * Estimates the heap size of decoded change stream events, in the same way as
 * {@link io.debezium.util.ApproximateStructSizeCalculator} does for source records.
 */
final class ApproximateBsonSizeCalculator {

    private static final int EMPTY_DOCUMENT_SIZE = 88;
    private static final int DOCUMENT_ENTRY_SIZE = 40;
    private static final int EMPTY_ARRAY_SIZE = 64;
    private static final int EMPTY_STRING_SIZE = 56;
    private static final int EMPTY_BYTES_SIZE = 24;
    private static final int EMPTY_PRIMITIVE = 24;
    private static final int REFERENCE_SIZE = 8;

    private ApproximateBsonSizeCalculator() {
    }

    static long getApproximateEventSize(ResumableChangeStreamEvent<?> event) {
        long size = getValueSize(event.resumeToken);
        if (event.document.isPresent()) {
            final ChangeStreamDocument<?> document = event.document.get();
            size += getObjectSize(document.getFullDocument());
            size += getObjectSize(document.getFullDocumentBeforeChange());
            size += getValueSize(document.getDocumentKey());
            final UpdateDescription updateDescription = document.getUpdateDescription();
            if (updateDescription != null) {
                size += getValueSize(updateDescription.getUpdatedFields());
                size += getStringsSize(updateDescription.getRemovedFields());
                size += getValueSize(updateDescription.getDisambiguatedPaths());
            }
        }
        return size;
    }

    private static long getObjectSize(Object value) {
        return value instanceof BsonValue bsonValue ? getValueSize(bsonValue) : 0L;
    }

    static long getValueSize(BsonValue value) {
        if (value == null) {
            return 0L;
        }
        return switch (value.getBsonType()) {
            case DOCUMENT -> getDocumentSize(value.asDocument());
            case ARRAY -> getArraySize(value.asArray());
            case STRING -> EMPTY_PRIMITIVE + getStringSize(value.asString().getValue());
            case SYMBOL -> EMPTY_PRIMITIVE + getStringSize(value.asSymbol().getSymbol());
            case JAVASCRIPT -> EMPTY_PRIMITIVE + getStringSize(value.asJavaScript().getCode());
            case JAVASCRIPT_WITH_SCOPE -> EMPTY_PRIMITIVE + getStringSize(value.asJavaScriptWithScope().getCode())
                    + getDocumentSize(value.asJavaScriptWithScope().getScope());
            case REGULAR_EXPRESSION -> EMPTY_PRIMITIVE + getStringSize(value.asRegularExpression().getPattern())
                    + getStringSize(value.asRegularExpression().getOptions());
            case BINARY -> EMPTY_PRIMITIVE + EMPTY_BYTES_SIZE + value.asBinary().getData().length;
            default -> EMPTY_PRIMITIVE;
        };
    }

    private static long getDocumentSize(BsonDocument document) {
        long size = EMPTY_DOCUMENT_SIZE;
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            size += DOCUMENT_ENTRY_SIZE;
            size += getStringSize(entry.getKey());
            size += getValueSize(entry.getValue());
        }
        return size;
    }

    private static long getArraySize(BsonArray array) {
        long size = EMPTY_ARRAY_SIZE;
        for (BsonValue element : array) {
            size += REFERENCE_SIZE;
            size += getValueSize(element);
        }
        return size;
    }

    private static long getStringsSize(List<String> strings) {
        if (strings == null) {
            return 0L;
        }
        long size = EMPTY_ARRAY_SIZE;
        for (String string : strings) {
            size += REFERENCE_SIZE + getStringSize(string);
        }
        return size;
    }

    private static long getStringSize(String string) {
        return string == null ? 0L : EMPTY_STRING_SIZE + string.length();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.DataException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.JsonReadFeature;

/**
 * Extracts selected JSON fragments without changing numeric literals, type wrappers, or string escapes.
 * Paths must also be resolved against BSON so Extended JSON wrappers are not treated as document fields.
 */
final class MongoDocumentJson {
    // The connector's legacy writer emits bare NaN and Infinity values.
    private static final JsonFactory FACTORY = JsonFactory.builder().enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS).build();

    private MongoDocumentJson() {
    }

    static Map<String, String> extract(String json, Set<String> paths) {
        final Map<String, String> fragments = new HashMap<>();
        final Map<String, Integer> starts = new HashMap<>();
        try (var parser = FACTORY.createParser(json)) {
            while (parser.nextToken() != null) {
                final var token = parser.currentToken();
                if (token == JsonToken.END_OBJECT && parser.getParsingContext().inRoot()) {
                    if (parser.nextToken() != null) {
                        throw new DataException("Unexpected trailing content in the incoming MongoDB document JSON");
                    }
                    break;
                }
                if (token == JsonToken.FIELD_NAME) {
                    continue;
                }
                final var path = parser.getParsingContext().pathAsPointer().toString();
                if (!paths.contains(path)) {
                    continue;
                }
                if (token.isStructStart()) {
                    starts.put(path, Math.toIntExact(parser.currentTokenLocation().getCharOffset()));
                }
                else {
                    // String tokens can be read lazily; finish them before taking the end offset.
                    parser.finishToken();
                    final int start = token.isStructEnd() ? starts.remove(path) : Math.toIntExact(parser.currentTokenLocation().getCharOffset());
                    final int end = Math.toIntExact(parser.currentLocation().getCharOffset());
                    fragments.put(path, json.substring(start, end));
                }
            }
        }
        catch (IOException e) {
            throw new DataException("Cannot read the incoming MongoDB document JSON", e);
        }
        return fragments;
    }
}

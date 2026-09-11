/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.config.ConfigException;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * An RFC 6901 JSON Pointer evaluated against the BSON document, before Connect conversion.
 */
final class MongoDocumentPath {
    private final List<String> tokens;

    MongoDocumentPath(String pointer) {
        List<String> parsed = new ArrayList<>();
        if (!pointer.isEmpty()) {
            if (!pointer.startsWith("/")) {
                throw new ConfigException("Document paths must be JSON Pointers beginning with '/', or empty for the whole document");
            }
            for (String token : pointer.substring(1).split("/", -1)) {
                for (int i = 0; i < token.length(); i++) {
                    if (token.charAt(i) == '~' && (++i == token.length() || (token.charAt(i) != '0' && token.charAt(i) != '1'))) {
                        throw new ConfigException("Invalid JSON Pointer escape; use '~0' for '~' and '~1' for '/'");
                    }
                }
                parsed.add(token.replace("~1", "/").replace("~0", "~"));
            }
        }
        tokens = List.copyOf(parsed);
    }

    boolean isRoot() {
        return tokens.isEmpty();
    }

    BsonValue read(BsonDocument document) {
        BsonValue value = document;
        for (String token : tokens) {
            if (value == null || value.isNull()) {
                return null;
            }
            if (value.isDocument()) {
                value = value.asDocument().get(token);
            }
            else if (value.isArray()) {
                if (!token.matches("0|[1-9][0-9]*")) {
                    return null;
                }
                final int index;
                try {
                    index = Integer.parseInt(token);
                }
                catch (NumberFormatException e) {
                    return null;
                }
                if (index >= value.asArray().size()) {
                    return null;
                }
                value = value.asArray().get(index);
            }
            else {
                return null;
            }
        }
        return value;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.ArrayList;
import java.util.List;

import io.debezium.annotation.Immutable;

/**
 * Unique identifier for a PostgreSQL type, supporting both simple and schema-qualified type names.
 * Handles PostgreSQL identifier quoting rules where identifiers can be quoted with double quotes.
 *
 * @author Debezium Authors
 */
@Immutable
public record TypeId(String schemaName, String typeName) {

    /**
     * Compact constructor with validation.
     */
    public TypeId {
        if (typeName == null) {
            throw new IllegalArgumentException("typeName cannot be null");
        }
    }

    /**
     * Parse the supplied string into a TypeId, handling PostgreSQL quoting rules.
     * PostgreSQL identifiers can be quoted with double quotes, and quotes within identifiers
     * are escaped by doubling them.
     *
     * @param str the string representation of the type identifier; may not be null
     * @return the type ID, or null if it could not be parsed
     */
    public static TypeId parse(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }

        final var parts = parseParts(str);

        if (parts.isEmpty()) {
            return null;
        }
        if (parts.size() == 1) {
            return new TypeId(null, parts.get(0)); // type only
        }
        return new TypeId(parts.get(0), parts.get(1)); // schema & type
    }

    /**
     * Parse a PostgreSQL identifier string into its component parts.
     * Handles quoted identifiers with double quotes.
     */
    private static List<String> parseParts(String str) {
        final var parts = new ArrayList<String>();
        final var current = new StringBuilder();
        var inQuotes = false;
        var i = 0;

        while (i < str.length()) {
            final var ch = str.charAt(i);

            if (ch == '"') {
                if (inQuotes && i + 1 < str.length() && str.charAt(i + 1) == '"') {
                    // Escaped quote - add one quote to the identifier
                    current.append('"');
                    i += 2;
                    continue;
                }
                // Toggle quote state
                inQuotes = !inQuotes;
                i++;
            }
            else if (ch == '.' && !inQuotes) {
                // Part separator
                if (current.length() > 0) {
                    parts.add(current.toString());
                    current.setLength(0);
                }
                i++;
            }
            else {
                current.append(ch);
                i++;
            }
        }

        // Add the last part
        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts;
    }

    /**
     * Get the name of the schema.
     *
     * @return the schema name, or null if the type does not belong to a schema
     */
    public String schema() {
        return schemaName;
    }

    /**
     * Get the full identifier string.
     *
     * @return the identifier string
     */
    public String identifier() {
        return typeId(schemaName, typeName);
    }

    /**
     * Returns a dot-separated String representation of this identifier, quoting all
     * name parts with the {@code "} char.
     */
    public String toDoubleQuotedString() {
        final var quoted = new StringBuilder();

        if (schemaName != null && !schemaName.isEmpty()) {
            quoted.append(quote(schemaName)).append(".");
        }

        quoted.append(quote(typeName));

        return quoted.toString();
    }

    @Override
    public String toString() {
        return identifier();
    }

    private static String typeId(String schema, String type) {
        if (schema == null || schema.isEmpty()) {
            return type;
        }
        return schema + "." + type;
    }

    /**
     * Quotes the given identifier part with double quotes.
     */
    private static String quote(String identifierPart) {
        if (identifierPart == null || identifierPart.isEmpty()) {
            return "\"\"";
        }

        if (identifierPart.charAt(0) != '"' && identifierPart.charAt(identifierPart.length() - 1) != '"') {
            // Escape any existing double quotes by doubling them
            final var escaped = identifierPart.replace("\"", "\"\"");
            return "\"" + escaped + "\"";
        }

        return identifierPart;
    }
}

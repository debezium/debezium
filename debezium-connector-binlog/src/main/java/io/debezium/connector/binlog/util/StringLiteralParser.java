/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.binlog.util;

import io.debezium.util.Strings;

/**
 * Decodes quoted string literal tokens used in MySQL and MariaDB DDL.
 */
public final class StringLiteralParser {

    private StringLiteralParser() {
    }

    /**
     * Decodes a single quoted token without a charset introducer.
     *
     * @param literal the quoted string token
     * @param noBackslashEscapes whether NO_BACKSLASH_ESCAPES is active
     * @return the unquoted and unescaped value
     */
    public static String parse(String literal, boolean noBackslashEscapes) {
        if (noBackslashEscapes) {
            return Strings.unquoteIdentifierPart(literal);
        }
        final var value = new StringBuilder();
        final char quote = literal.charAt(0);
        for (int i = 1; i < literal.length() - 1; i++) {
            final char current = literal.charAt(i);
            if (current == quote && i + 1 < literal.length() - 1 && literal.charAt(i + 1) == quote) {
                value.append(quote);
                i++;
            }
            else if (current == '\\' && i + 1 < literal.length() - 1) {
                final char escaped = literal.charAt(++i);
                switch (escaped) {
                    case '0' -> value.append('\0');
                    case 'b' -> value.append('\b');
                    case 'n' -> value.append('\n');
                    case 'r' -> value.append('\r');
                    case 't' -> value.append('\t');
                    case 'Z' -> value.append('\u001a');
                    // Outside LIKE patterns, these backslashes are preserved.
                    case '%', '_' -> value.append('\\').append(escaped);
                    default -> value.append(escaped);
                }
            }
            else {
                value.append(current);
            }
        }
        return value.toString();
    }

}

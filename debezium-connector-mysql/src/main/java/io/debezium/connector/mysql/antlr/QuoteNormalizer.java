/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.antlr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts double-quoted string literals to single-quoted strings in MySQL DDL.
 * The Oracle MySQL grammar uses ANSI_QUOTES mode (double quotes = identifiers),
 * but MySQL's default mode allows double quotes for strings. This normalizer
 * transforms DDL before parsing to support both modes.
 *
 * @author Debezium Authors
 */
public class QuoteNormalizer {

    // Regex group indices for clarity
    private static final int DOUBLE_QUOTED_STRING_GROUP = 1;
    private static final int BACKTICK_IDENTIFIER_GROUP = 2;

    /**
     * Pattern matches either:
     * - Group 1: Double-quoted strings  {@code "(content)"}
     * - Group 2: Backtick identifiers   {@code `identifier`}
     *
     */
    private static final Pattern QUOTED_LITERAL_PATTERN = Pattern.compile(
            "\"((?:[^\"\\\\]|\\\\.|\"\")*)\"|(`(?:``|[^`])*`)",
            Pattern.DOTALL);

    /**
     * Normalizes MySQL DDL by converting double-quoted strings to single-quoted strings
     * while preserving backtick-quoted identifiers.
     *
     * @param ddlContent The DDL statement to normalize
     * @return Normalized DDL with double-quoted strings converted to single quotes,
     *         or the original input if null or empty
     */
    public static String normalize(String ddlContent) {
        if (ddlContent == null || ddlContent.isEmpty()) {
            return ddlContent;
        }

        Matcher matcher = QUOTED_LITERAL_PATTERN.matcher(ddlContent);
        StringBuilder normalized = new StringBuilder(ddlContent.length());

        while (matcher.find()) {
            if (isBacktickIdentifier(matcher)) {
                preserveOriginal(matcher, normalized);
            }
            else {
                convertToSingleQuoted(matcher, normalized);
            }
        }
        matcher.appendTail(normalized);
        return normalized.toString();
    }

    /**
     * Checks if the current match is a backtick identifier.
     */
    private static boolean isBacktickIdentifier(Matcher matcher) {
        return matcher.group(BACKTICK_IDENTIFIER_GROUP) != null;
    }

    /**
     * Preserves the original matched text unchanged.
     */
    private static void preserveOriginal(Matcher matcher, StringBuilder result) {
        matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(0)));
    }

    /**
     * Converts a double-quoted string to single-quoted, escaping apostrophes.
     */
    private static void convertToSingleQuoted(Matcher matcher, StringBuilder result) {
        String escapedContent = matcher.group(DOUBLE_QUOTED_STRING_GROUP).replace("'", "''");
        matcher.appendReplacement(result, "'" + Matcher.quoteReplacement(escapedContent) + "'");
    }
}

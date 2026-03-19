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
 *
 * The Oracle MySQL grammar uses ANSI_QUOTES mode (double quotes = identifiers),
 * but MySQL's default mode allows double quotes for strings. This normalizer
 * transforms DDL before parsing to support both modes.
 *
 * Important: Preserves backtick-quoted identifiers unchanged to avoid modifying
 * double-quote characters inside identifiers (e.g., `myta""ble9`).
 */
public class QuoteNormalizer {

    // Matches: double-quoted strings (group 1) OR backtick identifiers (group 2)
    private static final Pattern DOUBLE_QUOTED_STRING = Pattern.compile(
            "\"((?:[^\"\\\\]|\\\\.|\"\")*" +
                    ")\"|(`(?:``|[^`])*`)",
            Pattern.DOTALL);

    /**
     * Normalizes DDL for grammar compatibility.
     *
     * @param ddlContent The DDL statement to normalize
     * @return Normalized DDL
     */
    public static String normalize(String ddlContent) {
        if (ddlContent == null || ddlContent.isEmpty()) {
            return ddlContent;
        }

        Matcher matcher = DOUBLE_QUOTED_STRING.matcher(ddlContent);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            if (matcher.group(2) != null) {
                // Backtick identifier - preserve unchanged
                matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(0)));
            }
            else {
                // Double-quoted string - convert to single quotes, escape apostrophes
                String content = matcher.group(1).replace("'", "''");
                matcher.appendReplacement(result, "'" + Matcher.quoteReplacement(content) + "'");
            }
        }
        matcher.appendTail(result);

        return result.toString();
    }
}

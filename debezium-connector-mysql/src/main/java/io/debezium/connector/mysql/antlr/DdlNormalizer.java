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
 * Also adds backticks around reserved keywords when used as identifiers.
 *
 * @author Debezium Authors
 */
public class DdlNormalizer {

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
     * Reserved keywords that need backticks when used as identifiers.
     */
    private static final Pattern RESERVED_KEYWORD_PATTERN = Pattern.compile(
            "\\b(CUME_DIST|DENSE_RANK|FIRST_VALUE|JSON_TABLE|LAG|LAST_VALUE|LEAD|NTH_VALUE|NTILE|PERCENT_RANK|RANK|ROW_NUMBER)\\b",
            Pattern.CASE_INSENSITIVE);

    /**
     * Normalizes MySQL DDL by converting double-quoted strings to single-quoted strings
     * while preserving backtick-quoted identifiers, and adding backticks around reserved
     * keywords when used as identifiers.
     *
     * @param ddlContent The DDL statement to normalize
     * @return Normalized DDL with double-quoted strings converted to single quotes
     *         and reserved keywords backtick-quoted, or the original input if null or empty
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

        String result = normalized.toString();
        result = addBackticksToReservedKeywords(result);

        return result;
    }

    private static boolean isBacktickIdentifier(Matcher matcher) {
        return matcher.group(BACKTICK_IDENTIFIER_GROUP) != null;
    }

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

    /**
     * Adds backticks around reserved keywords when used as identifiers (not as keywords).
     * Targets specific contexts: table names, column names, aliases, labels.
     */
    private static String addBackticksToReservedKeywords(String ddl) {
        String result = ddl;

        // Pattern for table names after CREATE TABLE, ALTER TABLE, DROP TABLE, FROM, JOIN, etc.
        result = result.replaceAll(
                "(?i)\\b(CREATE\\s+TABLE|ALTER\\s+TABLE|DROP\\s+TABLE|FROM|JOIN|LEFT\\s+JOIN|RIGHT\\s+JOIN|INNER\\s+JOIN|OUTER\\s+JOIN|INTO)\\s+(CUME_DIST|DENSE_RANK|FIRST_VALUE|JSON_TABLE|LAG|LAST_VALUE|LEAD|NTH_VALUE|NTILE|PERCENT_RANK|RANK|ROW_NUMBER)\\b",
                "$1 `$2`");

        // Pattern for column names in column definitions
        result = result.replaceAll(
                "(?i)\\b(CUME_DIST|DENSE_RANK|FIRST_VALUE|JSON_TABLE|LAG|LAST_VALUE|LEAD|NTH_VALUE|NTILE|PERCENT_RANK|RANK|ROW_NUMBER)\\s+((?:VAR)?CHAR|INT|BIGINT|DECIMAL|NUMERIC|FLOAT|DOUBLE|JSON|TEXT|BLOB|DATE|DATETIME|TIMESTAMP|TIME|YEAR|ENUM|SET|BINARY|VARBINARY|BIT|BOOL|BOOLEAN)\\b",
                "`$1` $2");

        // Pattern for labels in stored procedures: <keyword>:
        result = result.replaceAll(
                "(?i)\\b(CUME_DIST|DENSE_RANK|FIRST_VALUE|JSON_TABLE|LAG|LAST_VALUE|LEAD|NTH_VALUE|NTILE|PERCENT_RANK|RANK|ROW_NUMBER)\\s*:",
                "`$1`:");

        return result;
    }
}

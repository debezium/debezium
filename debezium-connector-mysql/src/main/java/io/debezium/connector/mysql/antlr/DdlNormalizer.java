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
 * When the server is running with {@code sql_mode=ANSI_QUOTES}, double-quoted
 * text represents identifiers and must not be converted. In that case, callers
 * should pass {@code ansiQuotesMode=true} to skip the conversion.
 *
 * Also adds backticks around keywords that the grammar cannot parse as unquoted
 * identifiers (see {@code KEYWORDS_REQUIRING_BACKTICKS}) when they are used as identifiers.
 *
 * @author Debezium Authors
 */
public class DdlNormalizer {

    // Regex group indices for clarity
    private static final int COMMENT_GROUP = 1;
    private static final int SINGLE_QUOTED_STRING_GROUP = 2;
    private static final int DOUBLE_QUOTED_STRING_GROUP = 3;
    private static final int BACKTICK_IDENTIFIER_GROUP = 4;

    /**
     * Pattern matches either:
     * - Group 1: SQL comments: {@code -- ...}, {@code # ...}, {@code /* ... * /} (but not {@code /*! ... * /})
     * - Group 2: Single-quoted strings  {@code '(content)'} (preserved as-is)
     * - Group 3: Double-quoted strings  {@code "(content)"}
     * - Group 4: Backtick identifiers   {@code `identifier`}
     *
     */
    private static final Pattern QUOTED_LITERAL_PATTERN = Pattern.compile(
            "(--[^\\n]*|#[^\\n]*|/\\*(?!!).*?\\*/)|(\'(?:\'\'|[^\'\\\\]|\\\\.)*\')|\"((?:[^\"\\\\]|\\\\.|\"\")*)\"|(`(?:``|[^`])*`)",
            Pattern.DOTALL);

    /**
     * Window function keywords (plus JSON_TABLE): reserved words since MySQL 8.0, but valid
     * unquoted identifiers on MySQL 5.7 and older, so DDL using them as identifiers can still
     * reach the parser (older sources, schema history). Missing from
     * {@code identifierKeywordsUnambiguous} in the migrated grammar.
     */
    private static final String WINDOW_FUNCTION_KEYWORDS = "CUME_DIST|DENSE_RANK|FIRST_VALUE|JSON_TABLE|LAG|LAST_VALUE|LEAD|NTH_VALUE|NTILE|PERCENT_RANK|RANK|ROW_NUMBER";

    /**
     * Version-gated keywords introduced by MySQL 8.0.32+ (URL) and 8.2.0+/9.x lexer gates:
     * mostly nonreserved keywords (URL, AUTO, MANUAL, PARALLEL, VECTOR) or not keywords at all
     * (ONLINE, OFFLINE) on real servers, so MySQL accepts them as unquoted identifiers — only
     * QUALIFY and TABLESAMPLE are reserved, from 8.2 on. Their tokens are missing from
     * {@code identifierKeywordsUnambiguous}, so the grammar rejects them in identifier
     * positions (debezium/dbz#2381). The upstream Oracle grammar in antlr/grammars-v4 has the
     * same omission; this group can be removed once the upstream fix (antlr/grammars-v4#4963)
     * is synced into the Debezium grammar.
     */
    private static final String VERSION_GATED_KEYWORDS = "AUTO|MANUAL|OFFLINE|ONLINE|PARALLEL|QUALIFY|TABLESAMPLE|URL|VECTOR";

    /**
     * All keywords the grammar cannot parse as unquoted identifiers; they are backtick-quoted
     * when they appear in identifier positions.
     */
    private static final String KEYWORDS_REQUIRING_BACKTICKS = WINDOW_FUNCTION_KEYWORDS + "|" + VERSION_GATED_KEYWORDS;

    private static final Pattern KEYWORDS_REQUIRING_BACKTICKS_PATTERN = Pattern.compile(
            "\\b(" + KEYWORDS_REQUIRING_BACKTICKS + ")\\b",
            Pattern.CASE_INSENSITIVE);

    /**
     * Table name positions: after CREATE/ALTER/DROP/RENAME/TRUNCATE TABLE (including
     * TEMPORARY and IF [NOT] EXISTS variants), FROM, JOIN, INTO, REFERENCES,
     * ON ({@code CREATE INDEX ... ON t}, {@code CREATE TRIGGER ... ON t}),
     * LIKE ({@code CREATE TABLE t2 LIKE t}),
     * and after TO ({@code RENAME TABLE ... TO t}, {@code RENAME COLUMN ... TO c}).
     */
    private static final Pattern TABLE_NAME_CONTEXT_PATTERN = Pattern.compile(
            "\\b(CREATE\\s+(?:TEMPORARY\\s+)?TABLE(?:\\s+IF\\s+NOT\\s+EXISTS)?|ALTER\\s+TABLE|DROP\\s+(?:TEMPORARY\\s+)?TABLE(?:\\s+IF\\s+EXISTS)?"
                    + "|RENAME\\s+TABLE|TRUNCATE(?:\\s+TABLE)?|REFERENCES|FROM|JOIN|INTO|TO|ON|LIKE)\\s+("
                    + KEYWORDS_REQUIRING_BACKTICKS + ")\\b",
            Pattern.CASE_INSENSITIVE);

    /**
     * Column clause positions: ADD/CHANGE/MODIFY/ALTER [COLUMN], DROP COLUMN, RENAME COLUMN,
     * and AFTER ({@code ADD COLUMN c INT AFTER c2}).
     */
    private static final Pattern COLUMN_CLAUSE_CONTEXT_PATTERN = Pattern.compile(
            "\\b(ADD(?:\\s+COLUMN)?|CHANGE(?:\\s+COLUMN)?|MODIFY(?:\\s+COLUMN)?|ALTER(?:\\s+COLUMN)?|DROP\\s+COLUMN|RENAME\\s+COLUMN|AFTER)\\s+("
                    + KEYWORDS_REQUIRING_BACKTICKS + ")\\b",
            Pattern.CASE_INSENSITIVE);

    /**
     * Column definitions: keyword directly followed by a data type.
     */
    private static final Pattern COLUMN_DEFINITION_CONTEXT_PATTERN = Pattern.compile(
            "\\b(" + KEYWORDS_REQUIRING_BACKTICKS + ")\\s+("
                    + "(?:VAR)?CHAR|CHARACTER|NCHAR|NVARCHAR|NATIONAL|(?:TINY|SMALL|MEDIUM|BIG)?INT|INTEGER|INT[12348]|MIDDLEINT"
                    + "|DECIMAL|DEC|FIXED|NUMERIC|FLOAT|FLOAT[48]|DOUBLE|REAL|SERIAL"
                    + "|JSON|(?:TINY|MEDIUM|LONG)?TEXT|(?:TINY|MEDIUM|LONG)?BLOB|LONG|DATE|DATETIME|TIMESTAMP|TIME|YEAR"
                    + "|ENUM|SET|BINARY|VARBINARY|BIT|BOOL|BOOLEAN|VECTOR"
                    + "|GEOMETRY|GEOMETRYCOLLECTION|POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON)\\b",
            Pattern.CASE_INSENSITIVE);

    /**
     * Index and constraint name positions: [ADD] [UNIQUE|FULLTEXT|SPATIAL] INDEX/KEY,
     * CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX, DROP INDEX, [ADD] CONSTRAINT,
     * DROP/ALTER CHECK.
     */
    private static final Pattern INDEX_OR_CONSTRAINT_CONTEXT_PATTERN = Pattern.compile(
            "\\b((?:ADD\\s+)?(?:UNIQUE\\s+|FULLTEXT\\s+|SPATIAL\\s+)?(?:INDEX|KEY)|CREATE\\s+(?:UNIQUE\\s+|FULLTEXT\\s+|SPATIAL\\s+)?INDEX|DROP\\s+INDEX|(?:ADD\\s+)?CONSTRAINT|(?:DROP|ALTER)\\s+CHECK)\\s+("
                    + KEYWORDS_REQUIRING_BACKTICKS + ")\\b",
            Pattern.CASE_INSENSITIVE);

    /**
     * Non-table schema object name positions: DATABASE/SCHEMA/VIEW/TRIGGER/PROCEDURE/
     * FUNCTION/EVENT (including IF [NOT] EXISTS variants), USE, and PARTITION names.
     * The anchor word always directly precedes the name, so leading verbs
     * ({@code CREATE OR REPLACE VIEW}, {@code REORGANIZE PARTITION}) need no handling.
     */
    private static final Pattern OBJECT_NAME_CONTEXT_PATTERN = Pattern.compile(
            "\\b((?:DATABASE|SCHEMA|VIEW|TRIGGER|PROCEDURE|FUNCTION|EVENT)(?:\\s+IF(?:\\s+NOT)?\\s+EXISTS)?|USE|PARTITION)\\s+("
                    + KEYWORDS_REQUIRING_BACKTICKS + ")\\b",
            Pattern.CASE_INSENSITIVE);

    /**
     * Labels in stored routines: {@code <keyword>:}.
     */
    private static final Pattern LABEL_CONTEXT_PATTERN = Pattern.compile(
            "\\b(" + KEYWORDS_REQUIRING_BACKTICKS + ")\\s*:",
            Pattern.CASE_INSENSITIVE);

    /**
     * Normalizes MySQL DDL by converting double-quoted strings to single-quoted strings
     * while preserving backtick-quoted identifiers, and adding backticks around
     * grammar-restricted keywords used as identifiers. Assumes the server is NOT in
     * ANSI_QUOTES mode.
     *
     * @param ddlContent The DDL statement to normalize
     * @return Normalized DDL with double-quoted strings converted to single quotes
     *         and grammar-restricted keywords backtick-quoted, or the original input
     *         if null or empty
     */
    public static String normalize(String ddlContent) {
        return normalize(ddlContent, false);
    }

    /**
     * Normalizes MySQL DDL by adding backticks around grammar-restricted keywords used
     * as identifiers. When {@code ansiQuotesMode} is false, also converts double-quoted
     * strings to single-quoted strings (for servers using the default sql_mode where
     * double quotes delimit string literals). When {@code ansiQuotesMode} is true,
     * double-quoted text is left as-is because it represents identifiers.
     *
     * @param ddlContent The DDL statement to normalize
     * @param ansiQuotesMode true if the server's sql_mode includes ANSI_QUOTES
     * @return Normalized DDL, or the original input if null or empty
     */
    public static String normalize(String ddlContent, boolean ansiQuotesMode) {
        if (ddlContent == null || ddlContent.isEmpty()) {
            return ddlContent;
        }

        Matcher matcher = QUOTED_LITERAL_PATTERN.matcher(ddlContent);
        StringBuilder normalized = new StringBuilder(ddlContent.length());
        int lastEnd = 0;

        while (matcher.find()) {
            normalized.append(addBackticksToReservedKeywords(ddlContent.substring(lastEnd, matcher.start())));
            if (isComment(matcher) || isSingleQuotedString(matcher) || isBacktickIdentifier(matcher)) {
                preserveOriginal(matcher, normalized);
            }
            else if (ansiQuotesMode) {
                preserveOriginal(matcher, normalized);
            }
            else {
                convertToSingleQuoted(matcher, normalized);
            }
            lastEnd = matcher.end();
        }
        normalized.append(addBackticksToReservedKeywords(ddlContent.substring(lastEnd)));

        return normalized.toString();
    }

    private static boolean isComment(Matcher matcher) {
        return matcher.group(COMMENT_GROUP) != null;
    }

    private static boolean isSingleQuotedString(Matcher matcher) {
        return matcher.group(SINGLE_QUOTED_STRING_GROUP) != null;
    }

    private static boolean isBacktickIdentifier(Matcher matcher) {
        return matcher.group(BACKTICK_IDENTIFIER_GROUP) != null;
    }

    private static void preserveOriginal(Matcher matcher, StringBuilder result) {
        result.append(matcher.group(0));
    }

    /**
     * Converts a double-quoted string to single-quoted, escaping apostrophes.
     */
    private static void convertToSingleQuoted(Matcher matcher, StringBuilder result) {
        String escapedContent = matcher.group(DOUBLE_QUOTED_STRING_GROUP).replace("'", "''");
        result.append("'").append(escapedContent).append("'");
    }

    /**
     * Adds backticks around the {@code KEYWORDS_REQUIRING_BACKTICKS} entries when used as
     * identifiers (not as keywords).
     * Targets specific contexts: table names, column clauses, column definitions,
     * index/constraint names, schema object names (database, view, trigger, routine,
     * event, partition) and labels.
     * Only ever invoked on the plain SQL segments between comments, string literals and
     * quoted identifiers, so their content is never rewritten.
     */
    private static String addBackticksToReservedKeywords(String ddl) {
        if (!KEYWORDS_REQUIRING_BACKTICKS_PATTERN.matcher(ddl).find()) {
            return ddl;
        }

        String result = TABLE_NAME_CONTEXT_PATTERN.matcher(ddl).replaceAll("$1 `$2`");
        result = COLUMN_CLAUSE_CONTEXT_PATTERN.matcher(result).replaceAll("$1 `$2`");
        result = COLUMN_DEFINITION_CONTEXT_PATTERN.matcher(result).replaceAll("`$1` $2");
        result = INDEX_OR_CONSTRAINT_CONTEXT_PATTERN.matcher(result).replaceAll("$1 `$2`");
        result = OBJECT_NAME_CONTEXT_PATTERN.matcher(result).replaceAll("$1 `$2`");
        result = LABEL_CONTEXT_PATTERN.matcher(result).replaceAll("`$1`:");

        return result;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.annotation.Immutable;

/**
 * A {@link TopicNamingStrategy} that maps table names using one or more patterns and a delegate naming strategy.
 * This can be used to map multiple tables each containing a separate shard into a single logical table name.
 * 
 * @author Randall Hauch
 */
@Immutable
public class ByTablePatternsTopicNamingStrategy implements TopicNamingStrategy {

    public static Function<String, String> mapRegex(String regexPatternWithReplacement) {
        String[] parts = regexPatternWithReplacement.split("(?<![?])[=]");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Unable to parse \"" + regexPatternWithReplacement + "\" into a regular expression and replacement");
        }
        return mapRegex(parts[0].trim(), parts[1].trim());
    }

    public static Function<String, String> mapRegex(String regexPattern, String replacement) {
        Pattern regex = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
        return (name) -> {
            Matcher matcher = regex.matcher(name);
            return matcher.matches() ? matcher.replaceAll(replacement) : null;
        };
    }

    private final Function<String, String> fqnMapper;
    private final String regexPatternAndReplacement;

    /**
     * Create the naming strategy that uses regular expressions and replacement strings to convert the fully-qualified table
     * names to topic names. The regular expressions are applied to the fully-qualified table names, and the first regex to
     * match is used.
     * 
     * @param regexPatternAndReplacement the mapping definition that consists of a regular expression, a " = " delimiter, and a
     *            replacement string that uses regex group numbers (e.g., "$1$2"); may not be null
     */
    public ByTablePatternsTopicNamingStrategy(String regexPatternAndReplacement) {
        this.regexPatternAndReplacement = regexPatternAndReplacement;
        this.fqnMapper = mapRegex(regexPatternAndReplacement);
    }

    @Override
    public String getTopic(String prefix, String databaseName, String tableName) {
        String fqn = databaseName + (tableName != null && tableName.length() != 0 ? ("." + tableName) : "");
        String result = fqnMapper.apply(fqn);
        return result == null ? null : prefix + DELIMITER + result;
    }

    @Override
    public String toString() {
        return "ByTablePatterns: " + regexPatternAndReplacement;
    }
}

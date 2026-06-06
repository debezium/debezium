/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A matcher that resolves field names for value-based dependencies.
 * This allows fields to declare dependencies using patterns or prefixes instead of explicit field name lists.
 *
 * @author Fiore Mario Vitale
 */
@FunctionalInterface
public interface DependentFieldMatcher {

    /**
     * Resolve this matcher against a set of available field names.
     *
     * @param allFieldNames all available field names in the configuration
     * @return list of field names that match this matcher's criteria
     */
    List<String> resolve(java.util.Set<String> allFieldNames);

    /**
     * Creates a matcher that matches field names exactly.
     * This is the most common matcher and is semantically equivalent to providing an explicit list.
     *
     * @param fieldNames the exact field names to match
     * @return a matcher that returns the specified field names
     */
    static DependentFieldMatcher exact(String... fieldNames) {
        return new ExactMatcher(fieldNames);
    }

    /**
     * Creates a matcher that matches field names by prefix.
     * Useful for matching all fields in a category (e.g., "log.mining.*").
     *
     * @param prefix the prefix that field names must start with
     * @return a matcher that returns all field names starting with the prefix
     */
    static DependentFieldMatcher withPrefix(String prefix) {
        return new PrefixMatcher(prefix);
    }

    /**
     * Matcher implementation that matches exact field names.
     */
    class ExactMatcher implements DependentFieldMatcher {
        private final List<String> fieldNames;

        public ExactMatcher(String... fieldNames) {
            this.fieldNames = Arrays.asList(fieldNames);
        }

        @Override
        public List<String> resolve(java.util.Set<String> allFieldNames) {
            return new ArrayList<>(fieldNames);
        }

        @Override
        public String toString() {
            return "ExactMatcher" + fieldNames;
        }
    }

    /**
     * Matcher implementation that matches field names by prefix.
     */
    class PrefixMatcher implements DependentFieldMatcher {
        private final String prefix;

        public PrefixMatcher(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public List<String> resolve(java.util.Set<String> allFieldNames) {
            return allFieldNames.stream()
                    .filter(name -> name.startsWith(prefix))
                    .sorted()
                    .collect(Collectors.toList());
        }

        @Override
        public String toString() {
            return "PrefixMatcher[" + prefix + "*]";
        }
    }
}

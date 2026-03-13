/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.externalize;

import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.debezium.function.Predicates;

public class ExternalizeColumnsPredicateBuilder {

    private String includeList;
    private String excludeList;

    public ExternalizeColumnsPredicateBuilder includeColumns(String columnSpec) {
        this.includeList = columnSpec;
        return this;
    }

    public ExternalizeColumnsPredicateBuilder excludeColumns(String columnSpec) {
        this.excludeList = columnSpec;
        return this;
    }

    public Predicate<String> build() {
        Predicate<String> includePredicate = (includeList == null || includeList.isBlank()) ? (s -> true) : Predicates.includes(toRegex(includeList));
        Predicate<String> excludePredicate = (excludeList == null || excludeList.isBlank()) ? (s -> false) : Predicates.includes(toRegex(excludeList));

        return includePredicate.and(excludePredicate.negate());
    }

    private String toRegex(String columnSpec) {
        if (columnSpec == null || columnSpec.trim().isEmpty()) {
            return null;
        }
        return Arrays.stream(columnSpec.split(","))
                .map(String::trim)
                .map(this::toPattern)
                .collect(Collectors.joining(","));
    }

    private String toPattern(String columnRef) {
        if (columnRef == null || columnRef.trim().isEmpty()) {
            return ".*";
        }
        return columnRef
                .replaceAll("\\.", "\\\\.")
                .replaceAll(":", "\\\\:")
                .replaceAll("\\*", ".*");
    }
}

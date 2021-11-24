/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.util.function.Predicate;

import io.debezium.function.Predicates;

public class LogicalDecodingMessageFilter {
    private final Predicate<String> filter;

    public LogicalDecodingMessageFilter(String inclusionString, String exclusionString) {
        Predicate<String> inclusions = notEmpty(inclusionString) ? Predicates.includes(inclusionString) : null;
        Predicate<String> exclusions = notEmpty(exclusionString) ? Predicates.excludes(exclusionString) : null;
        Predicate<String> filter = inclusions != null ? inclusions : exclusions;
        this.filter = filter != null ? filter : (id) -> true;
    }

    public Predicate<String> getFilter() {
        return filter;
    }

    public boolean isIncluded(String prefix) {
        return filter.test(prefix);
    }

    private boolean notEmpty(String value) {
        return value != null && !value.trim().isEmpty();
    }
}

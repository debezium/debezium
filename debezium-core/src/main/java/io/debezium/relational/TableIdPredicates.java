/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

/**
 * Collection of predicate methods used for parsing {@link TableId}.
 * As the special character may differ across databases or may not be supported at all,
 * given database implementation should eventually override the defaults.
 *
 * @author vjuranek
 */
public interface TableIdPredicates {

    default boolean isQuotingChar(char c) {
        return c == '"' || c == '\'' || c == '`';
    }

    default boolean isStartDelimiter(char c) {
        return false;
    }

    default boolean isEndDelimiter(char c) {
        return false;
    }
}

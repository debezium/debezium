/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

/**
 * Used by {@link SkipWhenKafkaVersion} and {@link SkipWhenDatabaseVersion} to define the type of skip rule.
 *
 * @author Chris Cranford
 */
public enum EqualityCheck {
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    EQUAL,
    GREATER_THAN_OR_EQUAL,
    GREATER_THAN
}

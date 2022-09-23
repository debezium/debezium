/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

/**
 * For system-versioned tables, represents the type of timestamp a column contains.
 *
 * @author Matt Howard
 * @see Column
 */
public enum PeriodDateType {
    START_DATE,
    END_DATE
}

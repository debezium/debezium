/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import org.apache.kafka.connect.data.Struct;

/**
 * A function that converts one change event row (from a snapshot select, or
 * from before/after state of a log event) into the corresponding Kafka Connect
 * key or value {@link Struct}.
 */
@FunctionalInterface
public interface StructGenerator {

    /**
     * Converts the given tuple into a corresponding change event key or value struct.
     */
    Struct generateValue(Object[] values);
}

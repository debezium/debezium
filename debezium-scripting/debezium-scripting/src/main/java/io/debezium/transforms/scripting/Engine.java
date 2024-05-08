/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.scripting;

import org.apache.kafka.connect.connector.ConnectRecord;

/**
 * The interface serves as an abstraction of expression language engine.
 *
 * @author Jiri Pechanec
 */
public interface Engine {

    /**
     * Pre-compiles the expression for repeated execution.
     * The method is called once upon the engine initialization.
     *
     * @param expression
     */
    void configure(String language, String expression);

    /**
     * Calculate a value out of the record.
     *
     * @param record to be used
     * @return result of calculation
     */
    <T> T eval(ConnectRecord<?> record, Class<T> type);
}

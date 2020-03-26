/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.filter;

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
    void parseExpression(String expression);

    /**
     * Evaluates whether the record should be filtered out or not by executing the expression.
     *
     * @param record to be evaluated
     * @return true to remove the record, false to keep it
     */
    boolean eval(ConnectRecord<?> record);
}

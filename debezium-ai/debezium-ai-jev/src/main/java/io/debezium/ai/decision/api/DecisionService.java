/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.api;

/**
 * Service which answers a set of questions over a state.
 * Implementations have to be safe for use from multiple threads.
 *
 * @author Jiri Pechanec
 */
public interface DecisionService extends AutoCloseable {

    /**
     * Evaluates all the questions of the request over its state.
     *
     * @throws DecisionException if the evaluation fails
     */
    DecisionResponse decide(DecisionRequest request) throws DecisionException;

    @Override
    void close();
}

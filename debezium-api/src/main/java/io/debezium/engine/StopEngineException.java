/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine;

import io.debezium.DebeziumException;

/**
 * An exception that is used to tell the engine to process the last source record and to then stop. When raised by
 * {@link Consumer} implementations passed to {@link DebeziumEngine.Builder#notifying(Consumer)}, this exception should
 * only be raised after that consumer has safely processed the passed event.
 *
 * @author Randall Hauch
 */
public class StopEngineException extends DebeziumException {

    private static final long serialVersionUID = 1L;

    public StopEngineException(String msg) {
        super(msg);
    }
}

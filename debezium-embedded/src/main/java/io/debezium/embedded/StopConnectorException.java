/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.function.Consumer;

import io.debezium.DebeziumException;

/**
 * An exception that is used to tell the connector to process the last source record and to then stop. When raised by
 * {@link Consumer} implementations passed to {@link EmbeddedEngine.Builder#notifying(Consumer)}, this exception should
 * only be raised after that consumer has safely processed the passed event.
 *
 * @author Randall Hauch
 */
@Deprecated
public class StopConnectorException extends DebeziumException {

    private static final long serialVersionUID = 1L;

    public StopConnectorException(String msg) {
        super(msg);
    }
}

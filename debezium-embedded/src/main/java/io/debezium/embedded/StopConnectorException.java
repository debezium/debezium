/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * An exception that is used to tell the connector to process the last source record and to then stop.
 * 
 * @author Randall Hauch
 */
public class StopConnectorException extends ConnectException {
    private static final long serialVersionUID = 1L;

    public StopConnectorException(String msg) {
        super(msg);
    }
}

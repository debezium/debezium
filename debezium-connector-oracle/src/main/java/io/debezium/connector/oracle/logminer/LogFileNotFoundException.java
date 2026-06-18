/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.DebeziumException;

/**
 * Identifies when log files could not be found.
 *
 * @author Chris Cranford
 */
public class LogFileNotFoundException extends DebeziumException {
    public LogFileNotFoundException(String message) {
        super(message);
    }
}

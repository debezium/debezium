/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium;

/**
 * Base exception thrown when a data conversion or data processing error occurs in Debezium.
 */
public class DebeziumDataException extends DebeziumException {

    private static final long serialVersionUID = 1L;

    public DebeziumDataException() {
    }

    public DebeziumDataException(String message) {
        super(message);
    }

    public DebeziumDataException(Throwable cause) {
        super(cause);
    }

    public DebeziumDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public DebeziumDataException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

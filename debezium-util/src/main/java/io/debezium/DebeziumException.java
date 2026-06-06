/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium;

/**
 * Base exception returned by Debezium API.
 *
 */
public class DebeziumException extends RuntimeException {

    private static final long serialVersionUID = -829914184849944524L;

    public DebeziumException() {
    }

    public DebeziumException(String message) {
        super(message);
    }

    public DebeziumException(Throwable cause) {
        super(cause);
    }

    public DebeziumException(String message, Throwable cause) {
        super(message, cause);
    }

    public DebeziumException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.spi;

/**
 * This exception is thrown when a retriable error occurs in the ChangeEventSink.
 */
public class ChangeEventSinkRetriableException extends RuntimeException {
    public ChangeEventSinkRetriableException(String message) {
        super(message);
    }

    public ChangeEventSinkRetriableException(String message, Throwable cause) {
        super(message, cause);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.spi;

import org.apache.kafka.connect.errors.RetriableException;

/**
 * This class provides a method to determine if an exception is retriable.
 */
public class SinkErrorHandler {

    /**
     * Determines if an exception is retriable.
     *
     * @param throwable The thrown exception.
     * @return True if the exception is retriable, false otherwise.
     */
    public static boolean isRetriable(Throwable throwable) {
        Throwable cause = throwable;

        while (cause != null) {
            if (cause instanceof RetriableException ||
                    cause instanceof ChangeEventSinkRetriableException) {
                return true;
            }
            cause = cause.getCause();
        }

        return false;
    }
}

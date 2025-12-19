/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.events;

/**
 * This event is fired after the Debezium engine completes it's shutdown. It includes all the
 * information about whether the prior execution was successful or if it failed, the reason
 * and error why.
 *
 * @author Chris Cranford
 */
public class DebeziumCompletionEvent {

    private final boolean success;
    private final String message;
    private final Throwable error;

    public DebeziumCompletionEvent(boolean success, String message, Throwable error) {
        this.success = success;
        this.message = message;
        this.error = error;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getError() {
        return error;
    }
}

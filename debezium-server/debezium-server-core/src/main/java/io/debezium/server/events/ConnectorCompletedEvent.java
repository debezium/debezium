/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.events;

import java.util.Optional;

/**
 * Fired when the connector was completed. Provides information about completion state, message
 * and optional stacktrace in case of error.
 *
 * @author Jiri Pechanec
 *
 */
public class ConnectorCompletedEvent {

    private final boolean success;
    private final String message;
    private final Throwable error;

    public ConnectorCompletedEvent(boolean success, String message, Throwable error) {
        this.success = success;
        this.message = message;
        this.error = error;
    }

    /**
     *
     * @return true if the connector was completed successfully
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     *
     * @return message associated with connection completion
     */
    public String getMessage() {
        return message;
    }

    /**
     *
     * @return optional error in case the connector has not started successfully or was terminated with an error
     */
    public Optional<Throwable> getError() {
        return Optional.ofNullable(error);
    }

    @Override
    public String toString() {
        return "ConnectorCompletedEvent [success=" + success + ", message=" + message + ", error=" + error + "]";
    }
}

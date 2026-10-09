/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

public record ConnectionValidationResult(boolean valid, String message, String errorType) {

    public ConnectionValidationResult(boolean valid) {

        this(valid, "", "");
    }

    public static ConnectionValidationResult failed(String message) {
        return new ConnectionValidationResult(false, message, "");
    }

    public static ConnectionValidationResult successful() {
        return new ConnectionValidationResult(true);
    }
}
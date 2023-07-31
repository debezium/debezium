/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.olr.client;

/**
 * An exception that represents a failure with the OpenLogReplicator network client.
 *
 * @author Chris Cranford
 */
public class OlrNetworkClientException extends RuntimeException {
    public OlrNetworkClientException(String message) {
        super(message);
    }

    public OlrNetworkClientException(String message, Throwable cause) {
        super(message, cause);
    }
}

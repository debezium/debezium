/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

/**
 * The role of the server that provides the streaming connection, as observed by the connector.
 */
public enum ServerRole {
    PRIMARY,
    REPLICA,
    UNKNOWN
}

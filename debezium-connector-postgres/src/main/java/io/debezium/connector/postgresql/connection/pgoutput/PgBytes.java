/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

/**
 * Just a data marker for byte[] that we don't want treat as strings.
 * @param bytes wrapped byte[]
 */
public record PgBytes(byte[] bytes) {
}

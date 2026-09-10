/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

/**
 * Identifies a {@link JdbcType} that binds Kafka Connect {@code BYTES} values as raw bytes.
 * Logical types that use a {@code BYTES} schema do not implement this interface.
 *
 * @author Minjae Lee
 */
public interface RawBytesJdbcType extends JdbcType {
}

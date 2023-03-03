/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

/**
 * An on-demand provider of a JDBC connection.
 *
 * @param <T>
 */
@FunctionalInterface
public interface ConnectionFactory<T extends JdbcConnection> {

    T newConnection();
}

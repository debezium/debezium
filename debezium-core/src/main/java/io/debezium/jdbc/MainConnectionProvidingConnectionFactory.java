/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

/**
 * An on-demand provider of a JDBC connection.
 * One of the connections is a specific and should be provided as a specific one.
 * Typically used in parallel snapshotting.
 *
 * @param <T>
 */
public interface MainConnectionProvidingConnectionFactory<T> extends ConnectionFactory<T> {

    T mainConnection();
}

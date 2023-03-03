/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

public interface MainConnectionProvidingConnectionFactory<T> extends ConnectionFactory<T> {

    T getMainConnection();
}

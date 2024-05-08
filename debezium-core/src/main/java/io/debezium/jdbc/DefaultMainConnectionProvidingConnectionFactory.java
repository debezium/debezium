/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

public class DefaultMainConnectionProvidingConnectionFactory<T extends JdbcConnection> implements MainConnectionProvidingConnectionFactory<T> {

    private final ConnectionFactory<T> delegate;

    private final T mainConnection;

    public DefaultMainConnectionProvidingConnectionFactory(ConnectionFactory<T> delegate) {
        this.delegate = delegate;
        this.mainConnection = delegate.newConnection();
    }

    @Override
    public T mainConnection() {
        return mainConnection;
    }

    @Override
    public T newConnection() {
        return delegate.newConnection();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

public class DefaultMainConnectionProvidingConnectionFactory<T> implements MainConnectionProvidingConnectionFactory<T> {

    private ConnectionFactory<T> delegate;

    private T mainConnection;

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

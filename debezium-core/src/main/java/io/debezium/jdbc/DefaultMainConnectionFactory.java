/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.jdbc;

public class DefaultMainConnectionFactory<T> implements MainConnectionFactory<T> {

    private ConnectionFactory<T> delegate;

    private T mainConnection;

    public DefaultMainConnectionFactory(ConnectionFactory<T> delegate) {
        this.delegate = delegate;
        this.mainConnection = delegate.newConnection();
    }

    @Override
    public T getMainConnection() {
        return mainConnection;
    }

    @Override
    public T newConnection() {
        return delegate.newConnection();
    }
}

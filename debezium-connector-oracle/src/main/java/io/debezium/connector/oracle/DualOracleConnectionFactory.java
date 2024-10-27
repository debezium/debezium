/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.jdbc.ConnectionFactory;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;

/**
 * Factory to manage between read-only and regular connection for the purpose of using only what is necessary for
 * start the connector while the read-only will be used to log mining.
 * @param <T>
 * @author Lucas Gazire
 */
public class DualOracleConnectionFactory<T extends JdbcConnection> implements MainConnectionProvidingConnectionFactory<T> {
    private final ConnectionFactory<T> mainConnectionFactory;
    private final ConnectionFactory<T> readOnlyConnectionFactory;
    private final T mainConnection;
    private final T readOnlyConnection;
    private final boolean isLogMiningReadonly;

    public DualOracleConnectionFactory(ConnectionFactory<T> mainConnectionFactory, ConnectionFactory<T> readOnlyConnectionFactory, OracleConnectorConfig config) {
        this.mainConnectionFactory = mainConnectionFactory;
        this.readOnlyConnectionFactory = readOnlyConnectionFactory;
        this.mainConnection = mainConnectionFactory.newConnection();
        this.readOnlyConnection = this.readOnlyConnectionFactory.newConnection();
        this.isLogMiningReadonly = config.isLogMiningReadOnly();
    }

    public T getConnection(){
        return this.isLogMiningReadonly ? this.readOnlyConnection : this.mainConnection;
    }

    @Override
    public T mainConnection() {
        return mainConnection;
    }

    @Override
    public T newConnection() {
        return mainConnectionFactory.newConnection();
    }

    public T newReadonlyConnection() {
        return readOnlyConnectionFactory.newConnection();
    }
}
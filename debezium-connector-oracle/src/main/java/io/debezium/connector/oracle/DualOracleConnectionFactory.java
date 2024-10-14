package io.debezium.connector.oracle;

import io.debezium.jdbc.ConnectionFactory;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;

public class DualOracleConnectionFactory<T extends JdbcConnection> implements MainConnectionProvidingConnectionFactory<T> {
    private final ConnectionFactory<T> mainConnectionFactory;
    private final ConnectionFactory<T> readonlyConnectionFactory;
    private final T mainConnection;
    private final T readonlyConnection;
    private final boolean isLogMiningReadonly;

    public DualOracleConnectionFactory(ConnectionFactory<T> mainConnectionFactory, ConnectionFactory<T> readonlyConnectionFactory,OracleConnectorConfig config) {
        this.mainConnectionFactory = mainConnectionFactory;
        this.readonlyConnectionFactory = readonlyConnectionFactory;
        this.mainConnection = mainConnectionFactory.newConnection();
        this.readonlyConnection = readonlyConnectionFactory.newConnection();
        this.isLogMiningReadonly = config.isLogMiningReadOnly();
    }

    public T getConnection(){
        return this.isLogMiningReadonly ? this.readonlyConnection : this.mainConnection;
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
        return readonlyConnectionFactory.newConnection();
    }


}

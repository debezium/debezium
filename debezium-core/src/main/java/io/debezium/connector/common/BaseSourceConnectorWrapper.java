package io.debezium.connector.common;

public abstract class BaseSourceConnectorWrapper<Config> implements SourceConnectorWrapper<Config> {

    protected ConnectorContextWrapper context;
}

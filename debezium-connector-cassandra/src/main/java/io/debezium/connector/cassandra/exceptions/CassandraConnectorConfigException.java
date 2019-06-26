/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.exceptions;

public class CassandraConnectorConfigException extends RuntimeException {
    public CassandraConnectorConfigException(String msg) {
        super(msg);
    }

    public CassandraConnectorConfigException(Throwable t) {
        super(t);
    }

    public CassandraConnectorConfigException(String msg, Throwable t) {
        super(msg, t);
    }
}

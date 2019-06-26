/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.exceptions;

public class CassandraConnectorTaskException extends RuntimeException {
    public CassandraConnectorTaskException(String msg) {
        super(msg);
    }

    public CassandraConnectorTaskException(Throwable t) {
        super(t);
    }

    public CassandraConnectorTaskException(String msg, Throwable t) {
        super(msg, t);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.exceptions;

public class CassandraConnectorSchemaException extends RuntimeException {
    public CassandraConnectorSchemaException(String msg) {
        super(msg);
    }

    public CassandraConnectorSchemaException(Throwable t) {
        super(t);
    }

    public CassandraConnectorSchemaException(String msg, Throwable t) {
        super(msg, t);
    }
}

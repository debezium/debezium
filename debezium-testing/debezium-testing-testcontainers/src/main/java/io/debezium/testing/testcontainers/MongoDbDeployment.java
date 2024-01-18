/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import org.testcontainers.lifecycle.Startable;

/**
 *  A MongoDB deployment with arbitrary topology
 */
public interface MongoDbDeployment extends Startable {

    /**
     * @return the <a href="https://www.mongodb.com/docs/manual/reference/connection-string/#standard-connection-string-format">standard connection string</a>
     * for this MongoDB deployment.
     */
    String getConnectionString();

    default String getAuthConnectionString(String username, String password, String authSource) {
        throw new UnsupportedOperationException("Authentication not supported for this deployment type");
    }

    default String getNoAuthConnectionString() {
        throw new UnsupportedOperationException("Authentication not supported for this deployment type");
    }

}

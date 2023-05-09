/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.junit;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.mongodb.ConnectionString;

import io.debezium.testing.testcontainers.MongoDbDeployment;

/**
 * Dummy implementation of {@link MongoDbDeployment} which simply wraps a
 * connection string to an arbitrary existing MongoDB deployment.
 */
public class MongoDbExternal implements MongoDbDeployment {
    public static final String MONGO_CONNECTION_STRING = "mongodb.connection.string";

    private final String connectionString;

    /**
     * Creates external {@link MongoDbDeployment} with connection string from {@link #MONGO_CONNECTION_STRING} property
     */
    public MongoDbExternal() {
        this(System.getProperty(MONGO_CONNECTION_STRING));
    }

    public MongoDbExternal(String connectionString) {
        this.connectionString = Objects.requireNonNull(connectionString, "Connection string is required for external MongoDB deployments");
    }

    @Override
    public String getConnectionString() {
        return connectionString;
    }

    public List<String> getHosts() {
        var cs = new ConnectionString(getConnectionString());
        return cs.getHosts().stream()
                .map(h -> h.split(":")[0])
                .collect(Collectors.toList());
    }

    @Override
    public void start() {
        // default no-op
    }

    @Override
    public void stop() {
        // default no-op
    }
}

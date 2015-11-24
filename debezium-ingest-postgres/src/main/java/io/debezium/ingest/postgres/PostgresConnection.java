/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ingest.postgres;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;

/**
 * A utility for working with MySQL connections.
 * @author Randall Hauch
 */
public class PostgresConnection extends JdbcConnection {
    
    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory("jdbc:postgresql://${hostname}:${port}/${dbname}");

    /**
     * Create a new instance with the given configuration and connection factory.
     * 
     * @param config the configuration; may not be null
     */
    public PostgresConnection(Configuration config) {
        super(config, FACTORY);
    }

    /**
     * Create a new instance with the given configuration and connection factory, and specify the operations that should be
     * run against each newly-established connection.
     * 
     * @param config the configuration; may not be null
     * @param initialOperations the initial operations that should be run on each new connection; may be null
     */
    public PostgresConnection(Configuration config, Operations initialOperations) {
        super(config, FACTORY, initialOperations);
    }
}

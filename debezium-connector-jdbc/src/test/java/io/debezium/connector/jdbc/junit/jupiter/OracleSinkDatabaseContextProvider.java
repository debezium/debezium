/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.containers.Network;

import io.debezium.connector.jdbc.junit.TestHelper;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    @SuppressWarnings("resource")
    public OracleSinkDatabaseContextProvider() {
        super(SinkType.ORACLE, new JdbcOracleContainer(false)
                .withNetwork(Network.SHARED)
                .withEnv("TZ", TestHelper.getSinkTimeZone()));
    }

}

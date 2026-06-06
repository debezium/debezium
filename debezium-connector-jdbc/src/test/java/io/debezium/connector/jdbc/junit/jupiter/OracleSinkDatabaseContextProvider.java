/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.jdbc.junit.TestHelper;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for Oracle.
 *
 * @author Chris Cranford
 */
public class OracleSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider {

    private static final DockerImageName IMAGE_NAME = DockerImageName.parse("quay.io/rh_integration/dbz-oracle:19.3.0")
            .asCompatibleSubstituteFor("gvenzl/oracle-xe");

    @SuppressWarnings("resource")
    public OracleSinkDatabaseContextProvider() {
        super(SinkType.ORACLE, new OracleContainer(IMAGE_NAME)
                .withNetwork(Network.SHARED)
                .withUsername("debezium")
                .withPassword("dbz")
                .withDatabaseName("ORCLPDB1")
                .withEnv("TZ", TestHelper.getSinkTimeZone()));
    }

}

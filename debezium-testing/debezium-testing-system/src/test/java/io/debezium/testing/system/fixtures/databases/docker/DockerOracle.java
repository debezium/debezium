/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.docker;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.oracle.DockerOracleDeployer;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { Network.class }, provides = { SqlDatabaseController.class })
public class DockerOracle extends DockerDatabaseFixture<SqlDatabaseController> {

    public DockerOracle(ExtensionContext.Store store) {
        super(SqlDatabaseController.class, store);
    }

    @Override
    protected SqlDatabaseController databaseController() throws Exception {
        Class.forName("oracle.jdbc.OracleDriver");
        DockerOracleDeployer deployer = new DockerOracleDeployer.Builder()
                .withNetwork(network)
                .build();
        return deployer.deploy();
    }
}

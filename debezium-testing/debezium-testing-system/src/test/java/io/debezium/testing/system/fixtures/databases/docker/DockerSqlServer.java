/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.docker;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.sqlserver.DockerSqlServerDeployer;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { Network.class }, provides = { SqlDatabaseController.class })
public class DockerSqlServer extends DockerDatabaseFixture<SqlDatabaseController> {

    public DockerSqlServer(ExtensionContext.Store store) {
        super(SqlDatabaseController.class, store);
    }

    @Override
    protected SqlDatabaseController databaseController() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        DockerSqlServerDeployer deployer = new DockerSqlServerDeployer.Builder()
                .withNetwork(network)
                .build();
        return deployer.deploy();
    }
}

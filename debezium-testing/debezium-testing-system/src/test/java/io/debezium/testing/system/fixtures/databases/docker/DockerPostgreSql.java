/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.docker;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_USERNAME;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.postgresql.DockerPostgreSqlDeployer;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { Network.class }, provides = { SqlDatabaseController.class })
public class DockerPostgreSql extends DockerDatabaseFixture<SqlDatabaseController> {

    public DockerPostgreSql(ExtensionContext.Store store) {
        super(SqlDatabaseController.class, store);
    }

    @Override
    protected SqlDatabaseController databaseController() throws Exception {
        Class.forName("org.postgresql.Driver");
        DockerPostgreSqlDeployer deployer = new DockerPostgreSqlDeployer.Builder()
                .withNetwork(network)
                .withContainerConfig(container -> {
                    container
                            .withUsername(DATABASE_POSTGRESQL_USERNAME)
                            .withPassword(DATABASE_POSTGRESQL_PASSWORD)
                            .withDatabaseName(DATABASE_POSTGRESQL_DBZ_DBNAME);
                })
                .build();
        return deployer.deploy();
    }
}

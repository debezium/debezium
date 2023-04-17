/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.docker;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_USERNAME;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import io.debezium.testing.system.tools.databases.mysql.DockerMySqlDeployer;
import io.debezium.testing.system.tools.databases.mysql.MySqlController;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { Network.class }, provides = { MySqlController.class })
public class DockerMySql extends DockerDatabaseFixture<MySqlController> {

    public DockerMySql(ExtensionContext.Store store) {
        super(MySqlController.class, store);
    }

    @Override
    protected MySqlController databaseController() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        DockerMySqlDeployer deployer = new DockerMySqlDeployer.Builder()
                .withNetwork(network)
                .withContainerConfig(container -> {
                    container
                            .withUsername(DATABASE_MYSQL_USERNAME)
                            .withPassword(DATABASE_MYSQL_PASSWORD)
                            .withExistingDatabase("inventory");
                }).build();
        return deployer.deploy();
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        dbController.initialize();

    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_MYSQL_USERNAME;

import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.mysql.DockerMySqlDeployer;

public interface DockerMySql
        extends TestSetupFixture, SqlDatabaseFixture, DockerNetwork {

    default void setupDatabase() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        DockerMySqlDeployer deployer = new DockerMySqlDeployer.Builder()
                .withNetwork(getNetwork())
                .withContainerConfig(container -> {
                    container
                            .withUsername(DATABASE_MYSQL_USERNAME)
                            .withPassword(DATABASE_MYSQL_PASSWORD)
                            .withExistingDatabase("inventory");
                })
                .build();
        SqlDatabaseController controller = deployer.deploy();
        setDbController(controller);
    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }

}

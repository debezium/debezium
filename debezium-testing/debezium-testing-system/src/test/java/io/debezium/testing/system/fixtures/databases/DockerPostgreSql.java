/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_DBZ_DBNAME;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_PASSWORD;
import static io.debezium.testing.system.tools.ConfigProperties.DATABASE_POSTGRESQL_USERNAME;

import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.postgresql.DockerPostgreSqlDeployer;

public interface DockerPostgreSql
        extends TestSetupFixture, SqlDatabaseFixture, DockerNetwork {

    default void setupDatabase() throws Exception {
        Class.forName("org.postgresql.Driver");
        DockerPostgreSqlDeployer deployer = new DockerPostgreSqlDeployer.Builder()
                .withNetwork(getNetwork())
                .withContainerConfig(container -> {
                    container
                            .withUsername(DATABASE_POSTGRESQL_USERNAME)
                            .withPassword(DATABASE_POSTGRESQL_PASSWORD)
                            .withDatabaseName(DATABASE_POSTGRESQL_DBZ_DBNAME);
                })
                .build();
        SqlDatabaseController controller = deployer.deploy();
        setDbController(controller);
    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }

}

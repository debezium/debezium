/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases;

import io.debezium.testing.system.fixtures.DockerNetwork;
import io.debezium.testing.system.fixtures.TestSetupFixture;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.oracle.DockerOracleDeployer;

public interface DockerOracle
        extends TestSetupFixture, SqlDatabaseFixture, DockerNetwork {

    default void setupDatabase() throws Exception {
        Class.forName("oracle.jdbc.OracleDriver");
        DockerOracleDeployer deployer = new DockerOracleDeployer.Builder()
                .withNetwork(getNetwork())
                .build();
        SqlDatabaseController controller = deployer.deploy();
        controller.initialize();
        setDbController(controller);
    }

    default void teardownDatabase() throws Exception {
        getDbController().reload();
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.mysql.OcpMySqlDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { SqlDatabaseController.class })
public class OcpMySql extends OcpDatabaseFixture<SqlDatabaseController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/mysql/deployment.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/mysql/service.yaml";

    public OcpMySql(ExtensionContext.Store store) {
        super(SqlDatabaseController.class, store);
    }

    @Override
    protected SqlDatabaseController databaseController() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        OcpMySqlDeployer deployer = new OcpMySqlDeployer.Deployer()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_MYSQL)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH)
                .build();
        return deployer.deploy();
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.informix.OcpInformixDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { SqlDatabaseController.class })
public class OcpInformix extends OcpDatabaseFixture<SqlDatabaseController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/informix/deployment.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/informix/service.yaml";

    public OcpInformix(ExtensionContext.Store store) {
        super(SqlDatabaseController.class, store);
    }

    @Override
    protected SqlDatabaseController databaseController() throws Exception {
        Class.forName("com.informix.jdbc.IfxDriver");
        OcpInformixDeployer deployer = new OcpInformixDeployer.Builder()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_INFORMIX)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH)
                .withPullSecrets(ConfigProperties.OCP_PULL_SECRET_PATH.get())
                .build();
        return deployer.deploy();
    }
}

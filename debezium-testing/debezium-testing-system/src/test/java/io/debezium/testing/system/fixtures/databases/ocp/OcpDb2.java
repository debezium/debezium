/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import static io.debezium.testing.system.tools.ConfigProperties.OCP_PULL_SECRET_NAME;
import static io.debezium.testing.system.tools.ConfigProperties.OCP_PULL_SECRET_PATH;
import static io.debezium.testing.system.tools.OpenShiftUtils.isRunningFromOcp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.SqlDatabaseController;
import io.debezium.testing.system.tools.databases.db2.OcpDB2Deployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { SqlDatabaseController.class })
public class OcpDb2 extends OcpDatabaseFixture<SqlDatabaseController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/db2/deployment.yaml";
    public static final String DB_SERVICE_PATH_LB = "/database-resources/db2/service-lb.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/db2/service.yaml";

    public OcpDb2(ExtensionContext.Store store) {
        super(SqlDatabaseController.class, store);
    }

    @Override
    protected SqlDatabaseController databaseController() throws Exception {
        Class.forName("com.ibm.db2.jcc.DB2Driver");
        OcpDB2Deployer.Builder deployerBuilder = new OcpDB2Deployer.Builder()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_DB2)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withLocalServices(DB_SERVICE_PATH)
                .withPublicServices(DB_SERVICE_PATH_LB);

        if (isRunningFromOcp()) {
            deployerBuilder.withExistingPullSecrets(ocp, OCP_PULL_SECRET_NAME.get());
        } else {
            deployerBuilder.withPullSecrets(OCP_PULL_SECRET_PATH.get());
        }

        return deployerBuilder.build().deploy();
    }
}

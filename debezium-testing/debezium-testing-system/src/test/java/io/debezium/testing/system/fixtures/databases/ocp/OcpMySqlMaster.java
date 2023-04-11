/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mysql.MySqlMasterController;
import io.debezium.testing.system.tools.databases.mysql.OcpMySqlMasterDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { MySqlMasterController.class })
public class OcpMySqlMaster extends OcpDatabaseFixture<MySqlMasterController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/mysql/master/master-deployment.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/mysql/master/master-service.yaml";
    private static final String DB_VOLUME_CLAIM_PATH = "/database-resources/mysql/master/volume-claim.yml";

    public OcpMySqlMaster(ExtensionContext.Store store) {
        super(MySqlMasterController.class, store);
    }

    @Override
    protected MySqlMasterController databaseController() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        OcpMySqlMasterDeployer deployer = new OcpMySqlMasterDeployer.Deployer()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_MYSQL)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withVolumeClaim(DB_VOLUME_CLAIM_PATH)
                .withPullSecrets(ConfigProperties.OCP_PULL_SECRET_PATH.get())
                .withServices(DB_SERVICE_PATH)
                .build();
        return deployer.deploy();
    }
}

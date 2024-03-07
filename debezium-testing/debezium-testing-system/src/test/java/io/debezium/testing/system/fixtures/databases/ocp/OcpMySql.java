/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import io.debezium.testing.system.assertions.JdbcAssertions;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mysql.MySqlController;
import io.debezium.testing.system.tools.databases.mysql.OcpMySqlDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { MySqlController.class, JdbcAssertions.class })
public class OcpMySql extends OcpDatabaseFixture<MySqlController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/mysql/master/master-deployment.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/mysql/master/master-service.yaml";
    private static final String DB_VOLUME_CLAIM_PATH = "/database-resources/mysql/master/volume-claim.yml";

    public OcpMySql(ExtensionContext.Store store) {
        super(MySqlController.class, store);
    }

    @Override
    public void setup() throws Exception {
        super.setup();
        store(JdbcAssertions.class, new JdbcAssertions(dbController));
    }

    @Override
    protected MySqlController databaseController() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        OcpMySqlDeployer deployer = new OcpMySqlDeployer.Deployer()
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

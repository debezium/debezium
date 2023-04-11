/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mysql.MySqlReplicaController;
import io.debezium.testing.system.tools.databases.mysql.OcpMySqlReplicaDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { MySqlReplicaController.class })
public class OcpMySqlReplica extends OcpDatabaseFixture<MySqlReplicaController> {
    public static final String DB_DEPLOYMENT_PATH = "/database-resources/mysql/replica/replica-deployment.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/mysql/replica/replica-service.yaml";

    public OcpMySqlReplica(ExtensionContext.Store store) {
        super(MySqlReplicaController.class, store);
    }

    @Override
    protected MySqlReplicaController databaseController() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        OcpMySqlReplicaDeployer deployer = new OcpMySqlReplicaDeployer.Deployer()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_MYSQL)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH)
                .withPullSecrets(ConfigProperties.OCP_PULL_SECRET_PATH.get())
                .build();
        return deployer.deploy();
    }
}

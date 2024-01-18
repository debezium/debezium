/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.postgresql.OcpPostgreSqlReplicaController;
import io.debezium.testing.system.tools.databases.postgresql.OcpPostgreSqlReplicaDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { OcpPostgreSqlReplicaController.class })
public class OcpPostgreSqlReplica extends OcpDatabaseFixture<OcpPostgreSqlReplicaController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/postgresql/replica/deployment.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/postgresql/replica/service.yaml";

    public OcpPostgreSqlReplica(ExtensionContext.Store store) {
        super(OcpPostgreSqlReplicaController.class, store);
    }

    @Override
    protected OcpPostgreSqlReplicaController databaseController() throws Exception {
        Class.forName("org.postgresql.Driver");
        OcpPostgreSqlReplicaDeployer deployer = new OcpPostgreSqlReplicaDeployer.Deployer()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_POSTGRESQL)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH)
                .build();
        return deployer.deploy();
    }
}

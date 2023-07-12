/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import java.util.List;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoShardedController;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoShardedDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { OcpMongoShardedController.class })
public class OcpMongoSharded extends OcpDatabaseFixture<OcpMongoShardedController> {
    public static final String MONGOS_DEPLOYMENT = "/database-resources/mongodb/sharded/deployment-mongos.yaml";
    public static final String CONFIG_DEPLOYMENT = "/database-resources/mongodb/sharded/deployment-config.yaml";

    // TODO refactor services
    public static final String[] SERVICES = List.of("/database-resources/mongodb/sharded/service-mongos.yaml",
            "/database-resources/mongodb/sharded/service-config.yaml").toArray(new String[0]);

    private OcpMongoShardedController controller;

    public OcpMongoSharded(ExtensionContext.Store store) {
        super(OcpMongoShardedController.class, store);
    }

    @Override
    protected OcpMongoShardedController databaseController() throws Exception {
        OcpMongoShardedDeployer deployer = new OcpMongoShardedDeployer.Deployer()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_MONGO)
                .withMongosDeployment(MONGOS_DEPLOYMENT)
                .withConfigDeployment(CONFIG_DEPLOYMENT)
                .withServices(SERVICES)
                .build();
        controller = deployer.deploy();
        return controller;
    }
}

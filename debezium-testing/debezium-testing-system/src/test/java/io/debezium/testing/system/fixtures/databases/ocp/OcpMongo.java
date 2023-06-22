/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.MongoDatabaseController;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { MongoDatabaseController.class })
public class OcpMongo extends OcpDatabaseFixture<MongoDatabaseController> {

    public static final String DB_DEPLOYMENT_PATH = "/database-resources/mongodb/standalone/deployment.yaml";
    public static final String DB_SERVICE_PATH = "/database-resources/mongodb/standalone/service.yaml";

    public OcpMongo(ExtensionContext.Store store) {
        super(MongoDatabaseController.class, store);
    }

    @Override
    protected MongoDatabaseController databaseController() throws Exception {
        OcpMongoDeployer deployer = new OcpMongoDeployer.Deployer()
                .withOcpClient(ocp)
                .withProject(ConfigProperties.OCP_PROJECT_MONGO)
                .withDeployment(DB_DEPLOYMENT_PATH)
                .withServices(DB_SERVICE_PATH)
                .build();
        return deployer.deploy();
    }
}

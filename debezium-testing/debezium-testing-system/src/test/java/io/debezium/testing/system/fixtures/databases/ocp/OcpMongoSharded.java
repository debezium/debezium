/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.databases.ocp;

import static io.debezium.testing.system.tools.databases.mongodb.sharded.MongoShardedUtil.getTestShardKeys;

import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.sharded.OcpMongoShardedConstants;
import io.debezium.testing.system.tools.databases.mongodb.sharded.OcpMongoShardedController;
import io.debezium.testing.system.tools.databases.mongodb.sharded.OcpMongoShardedDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { OcpMongoShardedController.class })
public class OcpMongoSharded extends OcpDatabaseFixture<OcpMongoShardedController> {

    private OcpMongoShardedController controller;

    public OcpMongoSharded(ExtensionContext.Store store) {
        super(OcpMongoShardedController.class, store);
    }

    @Override
    protected OcpMongoShardedController databaseController() throws Exception {
        OcpMongoShardedDeployer deployer = OcpMongoShardedDeployer.builder()
                .withProject(ConfigProperties.OCP_PROJECT_MONGO)
                .withOcp(ocp)
                .withConfigServerCount(OcpMongoShardedConstants.CONFIG_SERVER_REPLICAS)
                .withShardCount(OcpMongoShardedConstants.SHARD_COUNT)
                .withReplicaCount(OcpMongoShardedConstants.REPLICAS_IN_SHARD)
                .withShardKeys(getTestShardKeys())
                .withUseInternalAuth(ConfigProperties.DATABASE_MONGO_USE_KEYFILE)
                .withRootUser(ConfigProperties.DATABASE_MONGO_USERNAME, ConfigProperties.DATABASE_MONGO_SA_PASSWORD)
                .build();
        controller = deployer.deploy();
        return controller;
    }

    @Override
    public void teardown() throws Exception {
        controller.getMongo().stop();
        controller.getMongo().waitForStopped();
    }
}

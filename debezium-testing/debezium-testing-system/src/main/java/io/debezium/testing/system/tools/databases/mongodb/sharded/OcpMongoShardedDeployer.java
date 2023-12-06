/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.Deployer;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpMongoShardedDeployer implements Deployer<OcpMongoShardedController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoShardedDeployer.class);

    private final OpenShiftClient ocp;
    private final String project;
    private final int shardCount;
    private final int replicaCount;
    private final int configServerCount;
    private final String rootUserName;
    private final String rootPassword;
    private final boolean useInternalAuth;
    private List<MongoShardKey> shardKeys;

    private OcpMongoShardedCluster mongo;

    @lombok.Builder(setterPrefix = "with")
    public OcpMongoShardedDeployer(int shardCount, int replicaCount, int configServerCount, String rootUserName, String rootPassword, boolean useInternalAuth,
                                   OpenShiftClient ocp,
                                   String project, List<MongoShardKey> shardKeys) {
        this.shardCount = shardCount;
        this.replicaCount = replicaCount;
        this.configServerCount = configServerCount;
        this.rootUserName = rootUserName;
        this.rootPassword = rootPassword;
        this.useInternalAuth = useInternalAuth;
        this.ocp = ocp;
        this.project = project;
        this.shardKeys = shardKeys;
    }

    public OcpMongoShardedController getController() {
        return new OcpMongoShardedController(mongo, ocp, project);
    }

    @Override
    public OcpMongoShardedController deploy() throws Exception {
        LOGGER.info("Deploying sharded mongo cluster");
        mongo = OcpMongoShardedCluster.builder()
                .withOcp(ocp)
                .withProject(project)
                .withConfigServerCount(configServerCount)
                .withInitialShardCount(shardCount)
                .withReplicaCount(replicaCount)
                .withShardKeys(shardKeys)
                .withUseInternalAuth(useInternalAuth)
                .withRootUser(rootUserName, rootPassword)
                .withShardKeys(shardKeys)
                .build();
        mongo.start();
        return new OcpMongoShardedController(mongo, ocp, project);
    }

    public static class OcpMongoShardedDeployerBuilder {
        public OcpMongoShardedDeployerBuilder withRootUser(String rootUserName, String rootPassword) {
            this.rootUserName = rootUserName;
            this.rootPassword = rootPassword;
            return this;
        }
    }

}

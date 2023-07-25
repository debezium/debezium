/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.YAML;
import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.debezium.testing.system.tools.databases.mongodb.builders.OcpShardModelFactory;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpMongoShardedDeployer extends AbstractOcpDatabaseDeployer<OcpMongoShardedController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoShardedDeployer.class);

    private final Deployment mongosDeployment;
    private final Deployment configDeployment;

    private OcpMongoShardedDeployer(
                                    String project,
                                    Deployment mongosDeployment,
                                    Deployment configDeployment,
                                    List<Service> services,
                                    OpenShiftClient ocp) {
        super(project, null, services, ocp);
        this.mongosDeployment = mongosDeployment;
        this.configDeployment = configDeployment;
    }

    @Override
    public OcpMongoShardedController deploy() {
        if (pullSecret != null) {
            LOGGER.info("Deploying pull secrets");
            ocp.secrets().inNamespace(project).createOrReplace(pullSecret);
            ocpUtils.linkPullSecret(project, "default", pullSecret);
        }

        LOGGER.info("Deploying mongo config server");
        ocp.apps().deployments().inNamespace(project).createOrReplace(configDeployment);
        ocpUtils.waitForPods(project, configDeployment.getMetadata().getLabels());
        ocpUtils.createService(project, configDeployment.getMetadata().getName(), "db", OcpMongoShardedConstants.MONGO_CONFIG_PORT,
                configDeployment.getMetadata().getLabels(),
                configDeployment.getMetadata().getLabels());

        LOGGER.info("Deploying mongos router");
        deployment = ocp.apps().deployments().inNamespace(project).createOrReplace(mongosDeployment);
        ocpUtils.createService(project, mongosDeployment.getMetadata().getName(), "db", OcpMongoShardedConstants.MONGO_MONGOS_PORT,
                mongosDeployment.getMetadata().getLabels(),
                mongosDeployment.getMetadata().getLabels());

        LOGGER.info("Deploying mongo shards");
        List<Integer> shardRange = IntStream.rangeClosed(1, OcpMongoShardedConstants.SHARD_COUNT).boxed().collect(Collectors.toList());
        List<Integer> replicaRange = IntStream.rangeClosed(1, OcpMongoShardedConstants.REPLICAS_IN_SHARD).boxed().collect(Collectors.toList());

        shardRange.parallelStream().forEach((shardNum) -> replicaRange.parallelStream().forEach((replicaNum) -> {
            Deployment deployment1 = ocp
                    .apps()
                    .deployments()
                    .inNamespace(project)
                    .createOrReplace(OcpShardModelFactory.shardDeployment(shardNum, replicaNum));
            services.add(ocp.services()
                    .inNamespace(project)
                    .createOrReplace(OcpShardModelFactory.shardService(shardNum, replicaNum)));
            ocpUtils.waitForPods(project, deployment1.getMetadata().getLabels());
        }));
        LOGGER.info("Database deployed successfully");

        return getController(deployment, services, ocp);
    }

    @Override
    protected OcpMongoShardedController getController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpMongoShardedController(deployment, services, ocp);
    }

    public static class Deployer extends DatabaseBuilder<OcpMongoShardedDeployer.Deployer, OcpMongoShardedDeployer> {
        private Deployment mongosDeployment;
        private Deployment configDeployment;

        @Override
        public OcpMongoShardedDeployer build() {
            return new OcpMongoShardedDeployer(
                    project,
                    mongosDeployment,
                    configDeployment,
                    services,
                    ocpClient);
        }

        public OcpMongoShardedDeployer.Deployer withMongosDeployment(String deployment) {
            this.mongosDeployment = YAML.fromResource(deployment, Deployment.class);
            return self();
        }

        public OcpMongoShardedDeployer.Deployer withConfigDeployment(String deployment) {
            this.configDeployment = YAML.fromResource(deployment, Deployment.class);
            return self();
        }
    }
}

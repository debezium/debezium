/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.YAML;
import io.debezium.testing.system.tools.databases.AbstractOcpDatabaseDeployer;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

public class OcpMongoShardedDeployer extends AbstractOcpDatabaseDeployer<OcpMongoShardedController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpMongoShardedDeployer.class);

    private final List<Deployment> deployments;

    private OcpMongoShardedDeployer(
                                    String project,
                                    List<Deployment> deployments,
                                    List<Service> services,
                                    OpenShiftClient ocp) {
        super(project, null, services, ocp);
        this.deployments = deployments;
    }

    @Override
    public OcpMongoShardedController deploy() {
        LOGGER.info("Deploying pull secrets");

        if (pullSecret != null) {
            ocp.secrets().inNamespace(project).createOrReplace(pullSecret);
            ocpUtils.linkPullSecret(project, "default", pullSecret);
        }

        LOGGER.info("Deploying mongo pods");
        deployments.parallelStream().forEach(
                deployment -> {
                    deployment = ocp.apps().deployments().inNamespace(project).createOrReplace(deployment);
                    ocpUtils.waitForPods(project, deployment.getMetadata().getLabels());

                });
        deployment = ocp.apps().deployments().inNamespace(project).list().getItems().stream()
                .filter(d -> d.getMetadata().getName().equals("mongo-mongos"))
                .findFirst()
                .get();

        services = services.stream()
                .map(s -> ocp.services().inNamespace(project).createOrReplace(s))
                .collect(Collectors.toList());
        LOGGER.info("Database deployed successfully");

        return getController(deployment, services, ocp);
    }

    @Override
    protected OcpMongoShardedController getController(Deployment deployment, List<Service> services, OpenShiftClient ocp) {
        return new OcpMongoShardedController(deployment, services, ocp);
    }

    public static class Deployer extends DatabaseBuilder<OcpMongoShardedDeployer.Deployer, OcpMongoShardedDeployer> {
        private List<Deployment> deployments;

        @Override
        public OcpMongoShardedDeployer build() {
            return new OcpMongoShardedDeployer(
                    project,
                    deployments,
                    services,
                    ocpClient);
        }

        public DatabaseBuilder<OcpMongoShardedDeployer.Deployer, OcpMongoShardedDeployer> withDeployments(List<String> deployments) {
            this.deployments = new ArrayList<>();
            deployments.forEach(d -> this.deployments.add(YAML.fromResource(d, Deployment.class)));
            return self();
        }
    }
}

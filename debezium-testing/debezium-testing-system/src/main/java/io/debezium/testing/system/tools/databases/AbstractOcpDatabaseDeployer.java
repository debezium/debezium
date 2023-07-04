/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.YAML;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * @author Jakub Cechacek
 */
public abstract class AbstractOcpDatabaseDeployer<T> implements Deployer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOcpDatabaseDeployer.class);
    protected final OpenShiftClient ocp;
    protected final OpenShiftUtils ocpUtils;
    protected final String project;
    protected final Secret pullSecret;
    protected Deployment deployment;
    protected List<Service> services;

    public AbstractOcpDatabaseDeployer(
                                       String project,
                                       Deployment deployment,
                                       List<Service> services,
                                       Secret pullSecret,
                                       OpenShiftClient ocp) {
        this.ocp = ocp;
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.project = project;
        this.deployment = deployment;
        this.services = services;
        this.pullSecret = pullSecret;
    }

    public AbstractOcpDatabaseDeployer(
                                       String project,
                                       Deployment deployment,
                                       List<Service> services,
                                       OpenShiftClient ocp) {
        this(project, deployment, services, null, ocp);
    }

    @Override
    public T deploy() {
        if (pullSecret != null) {
            LOGGER.info("Deploying pull secrets");
            ocp.secrets().inNamespace(project).createOrReplace(pullSecret);
            ocpUtils.linkPullSecret(project, "default", pullSecret);
        }

        LOGGER.info("Deploying database");
        deployment = ocp.apps().deployments().inNamespace(project).createOrReplace(deployment);

        ocpUtils.waitForPods(project, deployment.getMetadata().getLabels());

        List<Service> svcs = services.stream()
                .map(s -> ocp.services().inNamespace(project).createOrReplace(s))
                .collect(Collectors.toList());
        LOGGER.info("Database deployed successfully");

        this.services = svcs;

        return getController(deployment, services, ocp);
    }

    protected abstract T getController(Deployment deployment, List<Service> services, OpenShiftClient ocp);

    static abstract public class DatabaseBuilder<B extends DatabaseBuilder<B, D>, D extends AbstractOcpDatabaseDeployer<?>>
            implements Deployer.Builder<B, D> {

        protected String project;
        protected Deployment deployment;
        protected List<Service> services;
        protected OpenShiftClient ocpClient;
        protected Secret pullSecret;

        public B withProject(String project) {
            this.project = project;
            return self();
        }

        public B withOcpClient(OpenShiftClient ocpClient) {
            this.ocpClient = ocpClient;
            return self();
        }

        public B withDeployment(String yamlPath) {
            this.deployment = YAML.fromResource(yamlPath, Deployment.class);
            return self();
        }

        public B withServices(String... yamlPath) {
            List<Service> services = Arrays.stream(yamlPath)
                    .map(p -> YAML.fromResource(p, Service.class))
                    .collect(Collectors.toList());
            return withServices(services);
        }

        public B withServices(Collection<Service> services) {
            this.services = new ArrayList<>(services);
            return self();
        }

        public B withPullSecrets(String yamlPath) {
            this.pullSecret = YAML.from(yamlPath, Secret.class);
            return self();
        }
    }
}

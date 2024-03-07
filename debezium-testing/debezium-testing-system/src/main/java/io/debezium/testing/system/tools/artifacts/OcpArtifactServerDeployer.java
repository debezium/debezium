/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.artifacts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.YAML;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

public class OcpArtifactServerDeployer extends AbstractOcpDeployer<OcpArtifactServerController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcpArtifactServerDeployer.class);
    private final OpenShiftUtils ocpUtils;
    private Deployment deployment;
    private Service service;

    public static class Builder implements Deployer.Builder<Builder, OcpArtifactServerDeployer> {

        private String project;
        private OpenShiftClient ocpClient;
        private OkHttpClient httpClient;
        private Deployment deployment;
        private Service service;

        public Builder withProject(String project) {
            this.project = project;
            return self();
        }

        public Builder withOcpClient(OpenShiftClient ocpClient) {
            this.ocpClient = ocpClient;
            return self();
        }

        public Builder withHttpClient(OkHttpClient httpClient) {
            this.httpClient = httpClient;
            return self();
        }

        public Builder withDeployment(String yamlPath) {
            this.deployment = YAML.fromResource(yamlPath, Deployment.class);
            return self();
        }

        public Builder withService(String yamlPath) {
            this.service = YAML.fromResource(yamlPath, Service.class);
            return self();
        }

        @Override
        public OcpArtifactServerDeployer build() {
            return new OcpArtifactServerDeployer(project, deployment, service, ocpClient, httpClient);
        }
    }

    public OcpArtifactServerDeployer(
                                     String project, Deployment deployment, Service service, OpenShiftClient ocp, OkHttpClient http) {
        super(project, ocp, http);
        this.ocpUtils = new OpenShiftUtils(ocp);
        this.deployment = deployment;
        this.service = service;
    }

    @Override
    public OcpArtifactServerController deploy() throws Exception {
        LOGGER.info("Deploying debezium artifact server");
        LOGGER.debug("Artifact server spec: \n" + deployment.getSpec().toString());
        deployment = ocp.apps().deployments().inNamespace(project).createOrReplace(deployment);

        service.getMetadata().setLabels(deployment.getMetadata().getLabels());
        service = ocp.services().inNamespace(project).createOrReplace(service);

        OcpArtifactServerController controller = new OcpArtifactServerController(deployment, service, ocp, http);
        controller.waitForServer();
        return controller;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

import static io.debezium.testing.system.tools.ConfigProperties.APICURIO_CRD_VERSION;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.apicurio.registry.operator.api.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.model.ApicurioRegistryList;
import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.YAML;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.model.annotation.Version;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

/**
 * Deployment management for Apicurio service registry OCP deployment
 * @author Jakub Cechacek
 */
public abstract class AbstractOcpApicurioDeployer<C extends RegistryController> extends AbstractOcpDeployer<C> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOcpApicurioDeployer.class);

    protected final String yamlPath;
    protected ApicurioRegistry registry;

    static {
        Version version = ApicurioRegistry.class.getAnnotation(Version.class);
        if (version == null || !APICURIO_CRD_VERSION.equalsIgnoreCase(version.value())) {
            throw new IllegalStateException("Incompatible Apicurio model version");
        }
    }

    public AbstractOcpApicurioDeployer(
                                       String project,
                                       String yamlPath,
                                       OpenShiftClient ocp,
                                       OkHttpClient http) {
        super(project, ocp, http);
        this.yamlPath = yamlPath;
    }

    @Override
    public C deploy() throws InterruptedException {
        LOGGER.info("Deploying Apicurio Registry from " + yamlPath);
        ApicurioRegistry registry = YAML.fromResource(yamlPath, ApicurioRegistry.class);
        registry = registryOperation().createOrReplace(registry);

        C controller = getController(registry);
        controller.waitForRegistry();

        return controller;
    }

    protected abstract C getController(ApicurioRegistry registry);

    protected abstract NonNamespaceOperation<ApicurioRegistry, ApicurioRegistryList, Resource<ApicurioRegistry>> registryOperation();

    public abstract static class RegistryBuilder<B extends RegistryBuilder<B, D>, D extends AbstractOcpApicurioDeployer<?>>
            implements Deployer.Builder<B, D> {

        protected String project;
        protected OpenShiftClient ocpClient;
        protected OkHttpClient httpClient;
        protected String yamlPath;

        public B withProject(String project) {
            this.project = project;
            return self();
        }

        public B withOcpClient(OpenShiftClient ocpClient) {
            this.ocpClient = ocpClient;
            return self();
        }

        public B withHttpClient(OkHttpClient httpClient) {
            this.httpClient = httpClient;
            return self();
        }

        public B withYamlPath(String yamlPath) {
            this.yamlPath = yamlPath;
            return self();
        }
    }
}

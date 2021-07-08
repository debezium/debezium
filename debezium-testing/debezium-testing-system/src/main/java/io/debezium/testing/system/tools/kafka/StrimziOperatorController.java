/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.OperatorController;
import io.debezium.testing.system.tools.YAML;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * This class provides control over Strimzi Cluster Operator deployed in OpenShift
 * @author Jakub Cechacek
 */
public class StrimziOperatorController extends OperatorController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaController.class);

    private final OpenShiftClient ocp;
    private final String project;
    private final String name;

    private Deployment operator;

    public static StrimziOperatorController forProject(String project, OpenShiftClient ocp) {
        Deployment operator = ocp.apps().deployments().inNamespace(project).withName("strimzi-cluster-operator").get();
        return new StrimziOperatorController(operator, ocp);
    }

    private StrimziOperatorController(Deployment operator, OpenShiftClient ocp) {
        super(operator, Collections.singletonMap("strimzi.io/kind", "cluster-operator"), ocp);
        this.operator = operator;
        this.name = operator.getMetadata().getName();
        this.project = operator.getMetadata().getNamespace();
        this.ocp = ocp;
    }

    /**
     * Sets image pull secrets for operands by setting STRIMZI_IMAGE_PULL_SECRETS environment variable
     */
    public void setOperandImagePullSecrets(String names) {
        setEnvVar("STRIMZI_IMAGE_PULL_SECRETS", names);
    }

    /**
     * Sets operator log level
     * @param level log leel
     */
    public void setLogLevel(String level) {
        setEnvVar("STRIMZI_LOG_LEVEL", level);
    }

    /**
     * Sets pull policy of the operator to 'Always'
     */
    public void setAlwaysPullPolicy() {
        LOGGER.info("Using 'Always' pull policy for all containers of deployment " + name + "'");
        List<Container> containers = operator.getSpec().getTemplate().getSpec().getContainers();
        containers.forEach(c -> c.setImagePullPolicy("Always"));
    }

    /**
     * Sets pull policy of operands to 'Always'
     */
    public void setOperandAlwaysPullPolicy() {
        setEnvVar("STRIMZI_IMAGE_PULL_POLICY", "Always");
    }

    /**
     * Deploys pull secret and links it to "default" service account in the project
     * @param yamlPath path to Secret descriptor
     * @return deployed pull secret
     */
    public Secret deployPullSecret(String yamlPath) {
        LOGGER.info("Deploying Secret from " + yamlPath);
        return ocp.secrets().inNamespace(project).createOrReplace(YAML.from(yamlPath, Secret.class));
    }

}

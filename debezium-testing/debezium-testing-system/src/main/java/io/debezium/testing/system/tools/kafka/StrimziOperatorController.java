/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.OperatorController;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * This class provides control over Strimzi Cluster Operator deployed in OpenShift
 * @author Jakub Cechacek
 */
public class StrimziOperatorController extends OperatorController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaController.class);
    private static final String DEPLOYMENT_NAME = "strimzi-cluster-operator";

    public static StrimziOperatorController forProject(String project, OpenShiftClient ocp) {
        Deployment operator = ocp.apps().deployments().inNamespace(project).withName("strimzi-cluster-operator").get();
        return new StrimziOperatorController(operator, ocp);
    }

    private StrimziOperatorController(Deployment operator, OpenShiftClient ocp) {
        super(operator, Collections.singletonMap("strimzi.io/kind", "cluster-operator"), ocp);
    }

    /**
     * Sets image pull secrets for operands by setting STRIMZI_IMAGE_PULL_SECRETS environment variable
     */
    public void setOperandImagePullSecrets(String names) {
        setEnvVar("STRIMZI_IMAGE_PULL_SECRETS", names);
    }

    /**
     * Sets image pull secrets for operands by setting STRIMZI_IMAGE_PULL_SECRETS environment variable
     */
    public void unsetOperandImagePullSecrets() {
        unsetEnvVar("STRIMZI_IMAGE_PULL_SECRETS");
    }

    /**
     * Sets operator log level
     * @param level log leel
     */
    public void setLogLevel(String level) {
        setEnvVar("STRIMZI_LOG_LEVEL", level);
    }

    /**
     * Sets pull policy of operands to 'Always'
     */
    public void setOperandAlwaysPullPolicy() {
        setEnvVar("STRIMZI_IMAGE_PULL_POLICY", "Always");
    }
}

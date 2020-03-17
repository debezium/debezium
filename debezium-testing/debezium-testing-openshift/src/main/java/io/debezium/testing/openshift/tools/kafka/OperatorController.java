/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.kafka;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentCondition;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * This class provides control over Strimzi Cluster Operator  deployed in OpenShift
 * @author Jakub Cechacek
 */
public class OperatorController {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaController.class);

    private final OpenShiftClient ocp;
    private final OpenShiftUtils ocpUtils;
    private String project;
    private Deployment operator;
    private String name;

    public OperatorController(Deployment operator, OpenShiftClient ocp) {
        this.operator = operator;
        this.name = operator.getMetadata().getName();
        this.project = operator.getMetadata().getNamespace();
        this.ocp = ocp;
        this.ocpUtils = new OpenShiftUtils(ocp);
    }

    /**
     * Sets image pull secret for operator's {@link Deployment} resource
     * @param secret name of the secret
     * @return {@link Deployment} resource of the operator
     */
    public Deployment setImagePullSecret(String secret) {
        LOGGER.info("Using " + secret + " as image pull secret for deployment '" + name + "'");
        List<LocalObjectReference> pullSecrets = Collections.singletonList(new LocalObjectReference(secret));
        ocpUtils.ensureHasPullSecret(operator, secret);
        return operator;
    }

    /**
     * Sets image pull secrets for operands by setting STRIMZI_IMAGE_PULL_SECRETS environment variable
     * @return {@link Deployment} resource of the operator
     */
    public Deployment setOperandImagePullSecrets(String names) {
        return setEnvVar("STRIMZI_IMAGE_PULL_SECRETS", names);
    }

    /**
     * Sets operator log level
     * @param level log leel
     * @return {@link Deployment} resource of the operator
     */
    public Deployment setLogLevel(String level) {
        return setEnvVar("STRIMZI_LOG_LEVEL", level);
    }

    public Deployment setAlwaysPullPolicy() {
        return setEnvVar("STRIMZI_IMAGE_PULL_POLICY", "Always");
    }

    /**
     * Set environment variable on all containers of operator's deployment
     * @param name variable's name
     * @param val variable's value
     * @return {@link Deployment} resource of the operator
     */
    public Deployment setEnvVar(String name, String val) {
        LOGGER.info("Setting variable " + name + "=" + val + " on deployment '" + name + "'");
        ocpUtils.ensureHasEnv(operator, new EnvVar(name, val, null));
        return operator;
    }

    /**
     * Updates Operator's {@link Deployment} resource
     * @return {@link Deployment} resource of the operator
     */
    public Deployment updateOperator() throws InterruptedException {
        operator = ocp.apps().deployments().inNamespace(project).createOrReplace(operator);
        operator = ocp.apps().deployments().inNamespace(project).withName(name)
                .waitUntilCondition(this::waitForAvailable, 5, MINUTES);
        return operator;
    }

    private boolean waitForAvailable(Deployment resource) {
        DeploymentStatus status = resource.getStatus();
        if (status == null) {
            return false;
        }
        Stream<DeploymentCondition> conditions = status.getConditions().stream();
        return conditions.anyMatch(c -> c.getType().equalsIgnoreCase("Available") && c.getStatus().equalsIgnoreCase("True"));
    }
}

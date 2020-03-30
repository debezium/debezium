/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools.kafka;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.openshift.tools.OpenShiftUtils;
import io.debezium.testing.openshift.tools.WaitConditions;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
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
     * Disables Strimzi cluster operator by scaling it to ZERO
     */
    public void disable() {
        LOGGER.info("Disabling Operator");
        setNumberOfReplicas(0);
        operator = ocp.apps().deployments().inNamespace(project).withName(name).createOrReplace(operator);
        await()
                .atMost(30, SECONDS)
                .pollDelay(5, SECONDS)
                .pollInterval(3, SECONDS)
                .until(() -> ocp.pods().inNamespace(project).withLabel("strimzi.io/kind", "cluster-operator").list().getItems().isEmpty());
    }

    /**
     * Enables Strimzi cluster operator by scaling it to ONE
     * @throws InterruptedException
     */
    public void enable() throws InterruptedException {
        LOGGER.info("Enabling Operator");
        setNumberOfReplicas(1);
        updateOperator();
    }

    /**
     * Sets number of replicas
     * @param replicas number of replicas
     */
    public void setNumberOfReplicas(int replicas) {
        LOGGER.info("Scaling Operator replicas to " + replicas);
        operator.getSpec().setReplicas(replicas);
    }

    /**
     * Semantic shortcut for calling {@link #setNumberOfReplicas(int)} with {@code 1} as value
     */
    public void setSingleReplica() {
        setNumberOfReplicas(1);
    }

    /**
     * Sets image pull secret for operator's {@link Deployment} resource
     * @param secret name of the secret
     */
    public void setImagePullSecret(String secret) {
        LOGGER.info("Using " + secret + " as image pull secret for deployment '" + name + "'");
        List<LocalObjectReference> pullSecrets = Collections.singletonList(new LocalObjectReference(secret));
        ocpUtils.ensureHasPullSecret(operator, secret);
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
     * Set environment variable on all containers of operator's deployment
     * @param name variable's name
     * @param val variable's value
     */
    public void setEnvVar(String name, String val) {
        LOGGER.info("Setting variable " + name + "='" + val + "' on deployment '" + this.name + "'");
        ocpUtils.ensureHasEnv(operator, new EnvVar(name, val, null));
    }

    /**
     * Updates Operator's {@link Deployment} resource
     * @return {@link Deployment} resource of the operator
     */
    public Deployment updateOperator() throws InterruptedException {
        operator = ocp.apps().deployments().inNamespace(project).createOrReplace(operator);
        operator = waitForAvailable();
        return operator;
    }

    private Deployment waitForAvailable() throws InterruptedException {
        return ocp.apps().deployments().inNamespace(project).withName(name).waitUntilCondition(WaitConditions::deploymentAvailableCondition, 5, MINUTES);
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * This class provides control over Strimzi Cluster Operator  deployed in OpenShift
 * @author Jakub Cechacek
 */
public class OperatorController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorController.class);

    protected final OpenShiftClient ocp;
    protected final OpenShiftUtils ocpUtils;
    protected final Map<String, String> podLabels;
    protected String project;
    protected Deployment operator;
    protected String name;
    private Secret pullSecret;

    public OperatorController(Deployment operator, Map<String, String> podLabels, OpenShiftClient ocp) {
        this.operator = operator;
        this.podLabels = podLabels;
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
                .atMost(scaled(30), SECONDS)
                .pollDelay(5, SECONDS)
                .pollInterval(3, SECONDS)
                .until(() -> ocp.pods().inNamespace(project).withLabels(podLabels).list().getItems().isEmpty());
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
        ocpUtils.ensureHasPullSecret(operator, secret);
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
     * Set environment variable on all containers of operator's deployment
     * @param name variable's name
     * @param val variable's value
     */
    public void setEnvVar(String name, String val) {
        LOGGER.info("Setting variable " + name + "='" + val + "' on deployment '" + this.name + "'");
        ocpUtils.ensureHasEnv(operator, new EnvVar(name, val, null));
    }

    public void unsetEnvVar(String name) {
        LOGGER.info("Unsetting variable " + name + "' on deployment '" + this.name + "'");
        ocpUtils.ensureNoEnv(operator, name);
    }

    /**
     * Updates Operator's {@link Deployment} resource
     * @return {@link Deployment} resource of the operator
     */
    public Deployment updateOperator() {
        operator = ocp.apps().deployments().inNamespace(project).createOrReplace(operator);
        operator = waitForAvailable();
        return operator;
    }

    /**
     * Deploys pull secret
     * @param yamlPath path to Secret descriptor
     * @return deployed pull secret
     */
    public Secret deployPullSecret(String yamlPath) {
        LOGGER.info("Deploying Secret from " + yamlPath);
        this.pullSecret = ocp.secrets().inNamespace(project).createOrReplace(YAML.from(yamlPath, Secret.class));

        String secretName = getPullSecretName();
        ocpUtils.linkPullSecret(project, "default", secretName);
        ocpUtils.linkPullSecret(project, "builder", secretName);
        setImagePullSecret(secretName);

        return pullSecret;
    }

    /**
     * Gets pull secret
     * @return pull secret associated with this operator
     */
    public Optional<Secret> getPullSecret() {
        return Optional.ofNullable(pullSecret);
    }

    /**
     * Gets pull secret name
     * @return name of the pull secret associated with this operator
     */
    public String getPullSecretName() {
        return getPullSecret().map(ps -> ps.getMetadata().getName()).orElse(null);
    }

    private Deployment waitForAvailable() {
        return ocp.apps().deployments().inNamespace(project).withName(name)
                .waitUntilCondition(WaitConditions::deploymentAvailableCondition, scaled(5), MINUTES);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.OperatorController;
import io.debezium.testing.system.tools.operatorutil.OpenshiftOperatorEnum;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * This class provides control over Strimzi Cluster Operator deployed in OpenShift
 * @author Jakub Cechacek
 */
public class StrimziOperatorController extends OperatorController {
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpKafkaController.class);
    private static final Map<String, String> OPERATOR_LABELS = Map.of("strimzi.io/kind", "cluster-operator");

    public static StrimziOperatorController forProject(String project, OpenShiftClient ocp) {
        LOGGER.info("Looking for " + OpenshiftOperatorEnum.STRIMZI.getName() + " operator");
        var ocpUtils = new OpenShiftUtils(ocp);
        Optional<Deployment> operator = ocpUtils.deploymentsWithPrefix(project, "amq-streams", "strimzi");

        return new StrimziOperatorController(operator.orElseThrow(), ocp);
    }

    private StrimziOperatorController(Deployment operator, OpenShiftClient ocp) {
        super(operator, OPERATOR_LABELS, ocp);
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

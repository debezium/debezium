/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.operator;

import static io.debezium.testing.system.tools.ConfigProperties.PREPARE_NAMESPACES_AND_STRIMZI;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.kafka.StrimziOperatorController;
import io.debezium.testing.system.tools.kafka.StrimziOperatorDeployer;
import io.debezium.testing.system.tools.operatorutil.OpenshiftOperatorEnum;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { StrimziOperatorController.class })
public class OcpStrimziOperator extends TestFixture {
    private final OpenShiftClient ocp;
    private final String project;
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpStrimziOperator.class);

    public OcpStrimziOperator(@NotNull ExtensionContext.Store store) {
        super(store);
        this.ocp = retrieve(OpenShiftClient.class);
        this.project = ConfigProperties.OCP_PROJECT_DBZ;
    }

    @Override
    public void setup() throws Exception {
        // in case productised amq streams is used - skip operator creation
        if (PREPARE_NAMESPACES_AND_STRIMZI) {
            StrimziOperatorController operatorController = new StrimziOperatorDeployer(project, ocp, null).deploy();
            operatorController.waitForAvailable();
        }
        else {
            LOGGER.info("Skipping " + OpenshiftOperatorEnum.STRIMZI.getName() + " deployment");
        }
        StrimziOperatorController operatorController = StrimziOperatorController.forProject(project, ocp);
        updateStrimziOperator(operatorController);
        store(StrimziOperatorController.class, operatorController);
    }

    @Override
    public void teardown() {
        // no-op: strimzi operator is reused across tests
        LOGGER.debug("Skipping " + OpenshiftOperatorEnum.STRIMZI.getName() + " operator tear down");
    }

    private void updateStrimziOperator(StrimziOperatorController operatorController) {
        operatorController.setLogLevel("DEBUG");
        operatorController.setAlwaysPullPolicy();
        operatorController.setOperandAlwaysPullPolicy();
        operatorController.setSingleReplica();

        ConfigProperties.OCP_PULL_SECRET_PATH.ifPresent(operatorController::deployPullSecret);

        operatorController.updateOperator();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.fixtures.operator;

import static io.debezium.testing.system.tools.ConfigProperties.OCP_PROJECT_REGISTRY;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.operatorutil.OpenshiftOperatorEnum;
import io.debezium.testing.system.tools.registry.ApicurioOperatorController;
import io.debezium.testing.system.tools.registry.ApicurioOperatorDeployer;
import io.fabric8.openshift.client.OpenShiftClient;

import fixture5.TestFixture;
import fixture5.annotations.FixtureContext;

@FixtureContext(requires = { OpenShiftClient.class }, provides = { ApicurioOperatorController.class })
public class OcpApicurioOperator extends TestFixture {
    private final OpenShiftClient ocp;
    private final String project;
    private static final Logger LOGGER = LoggerFactory.getLogger(OcpApicurioOperator.class);

    public OcpApicurioOperator(@NotNull ExtensionContext.Store store) {
        super(store);
        this.ocp = retrieve(OpenShiftClient.class);
        this.project = OCP_PROJECT_REGISTRY;
    }

    @Override
    public void setup() throws Exception {
        ApicurioOperatorController controller = new ApicurioOperatorDeployer(project, ocp, null).deploy();
        updateApicurioOperator(controller);
        store(controller);
    }

    @Override
    public void teardown() {
        // no-op: apicurio operator is reused across tests
        LOGGER.debug("Skipping " + OpenshiftOperatorEnum.APICURIO.getName() + " operator tear down");
    }

    private void updateApicurioOperator(ApicurioOperatorController operatorController) {
        ConfigProperties.OCP_PULL_SECRET_PATH.ifPresent(operatorController::deployPullSecret);

        operatorController.updateOperator();
    }
}

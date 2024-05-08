/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

import java.util.Collections;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.OperatorController;
import io.debezium.testing.system.tools.operatorutil.OpenshiftOperatorEnum;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * This class provides control over Apicurio Cluster Operator deployed in OpenShift
 * @author Jakub Cechacek
 */
public class ApicurioOperatorController extends OperatorController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApicurioOperatorController.class);

    public static ApicurioOperatorController forProject(String project, OpenShiftClient ocp) {
        Optional<Deployment> operator = ocp.apps()
                .deployments()
                .inNamespace(project)
                .list()
                .getItems()
                .stream()
                .filter(o -> o.getMetadata().getName().contains(OpenshiftOperatorEnum.APICURIO.getDeploymentNamePrefix()))
                .findFirst();
        if (operator.isEmpty()) {
            throw new IllegalStateException("Apicurio operator deployment not found");
        }
        return new ApicurioOperatorController(operator.get(), ocp);
    }

    private ApicurioOperatorController(Deployment operator, OpenShiftClient ocp) {
        super(operator, Collections.singletonMap("apicur.io/type", "operator"), ocp);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

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
public class ApicurioOperatorController extends OperatorController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApicurioOperatorController.class);
    private static final String DEPLOYMENT_NAME = "apicurio-registry-operator";

    public static ApicurioOperatorController forProject(String project, OpenShiftClient ocp) {
        Deployment operator = ocp.apps().deployments().inNamespace(project).withName(DEPLOYMENT_NAME).get();
        return new ApicurioOperatorController(operator, ocp);
    }

    private ApicurioOperatorController(Deployment operator, OpenShiftClient ocp) {
        super(operator, Collections.singletonMap("apicur.io/type", "operator"), ocp);
    }
}

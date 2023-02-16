/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.operatorutil.OpenshiftOperatorEnum;
import io.debezium.testing.system.tools.operatorutil.OperatorUtil;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

public class ApicurioOperatorDeployer extends AbstractOcpDeployer<ApicurioOperatorController> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApicurioOperatorDeployer.class);

    public ApicurioOperatorDeployer(String project, OpenShiftClient ocp, OkHttpClient http) {
        super(project, ocp, http);
    }

    @Override
    public ApicurioOperatorController deploy() throws Exception {
        LOGGER.info("Deploying apicurio operator to project: " + project);
        OperatorUtil.deployOperator(ocp, OpenshiftOperatorEnum.APICURIO, project);
        return ApicurioOperatorController.forProject(project, ocp);
    }
}

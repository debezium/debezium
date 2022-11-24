/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.AbstractOcpDeployer;
import io.debezium.testing.system.tools.operatorutil.OpenshiftOperatorEnum;
import io.debezium.testing.system.tools.operatorutil.OperatorUtil;
import io.fabric8.openshift.client.OpenShiftClient;

import okhttp3.OkHttpClient;

public class StrimziOperatorDeployer extends AbstractOcpDeployer<StrimziOperatorController> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziOperatorDeployer.class);

    public StrimziOperatorDeployer(String project, OpenShiftClient ocp, OkHttpClient http) {
        super(project, ocp, http);
    }

    @Override
    public StrimziOperatorController deploy() throws Exception {
        LOGGER.info("Deploying " + OpenshiftOperatorEnum.STRIMZI.getName() + " operator to project " + project);
        OperatorUtil.deployOperator(ocp, OpenshiftOperatorEnum.STRIMZI, project);
        return StrimziOperatorController.forProject(project, ocp);
    }
}

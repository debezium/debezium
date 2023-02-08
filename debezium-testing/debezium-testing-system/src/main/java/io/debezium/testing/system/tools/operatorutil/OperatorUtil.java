/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.operatorutil;

import static io.debezium.testing.system.tools.ConfigProperties.PRODUCT_BUILD;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.fabric8.OperatorSubscriptionBuilder;
import io.debezium.testing.system.tools.kafka.builders.StrimziSubscriptionBuilder;
import io.debezium.testing.system.tools.registry.builders.ApicurioSubscriptionBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * Methods shared in Strimzi and Apicurio operator deployment
 */
public class OperatorUtil {

    /**
     * Construct operator subscription instance and install operator to given namespace and wait for its install
     * @param ocp
     * @param operatorEnum
     * @param namespace
     * @throws InterruptedException
     */
    public static void deployOperator(OpenShiftClient ocp, OpenshiftOperatorEnum operatorEnum, String namespace) throws InterruptedException {
        OpenShiftUtils utils = new OpenShiftUtils(ocp);
        utils.createOrReplaceOperatorGroup(namespace, operatorEnum.getOperatorGroupName());

        OperatorSubscriptionBuilder sb;
        if (operatorEnum == OpenshiftOperatorEnum.STRIMZI) {
            sb = StrimziSubscriptionBuilder.base();
        }
        else {
            sb = ApicurioSubscriptionBuilder.base();
        }

        if (PRODUCT_BUILD) {
            sb.withProductConfig();
        }
        else {
            sb.withCommunityConfig();
        }

        sb.withChannel(operatorEnum.getSubscriptionUpdateChannel())
                .withNamespace(namespace);

        ocp.operatorHub().subscriptions().inNamespace(namespace).createOrReplace(sb.build());
        utils.waitForOperatorDeploymentExists(namespace, operatorEnum);
    }
}

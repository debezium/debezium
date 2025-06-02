/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.operatorutil;

import static io.debezium.testing.system.tools.ConfigProperties.PRODUCT_BUILD;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.fabric8.OperatorSubscriptionBuilder;
import io.debezium.testing.system.tools.kafka.builders.StrimziSubscriptionBuilder;
import io.debezium.testing.system.tools.registry.builders.ApicurioSubscriptionBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.InstallPlan;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * Methods shared in Strimzi and Apicurio operator deployment
 */
public class OperatorUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorUtil.class);

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
            sb = StrimziSubscriptionBuilder.base().withConfig(PRODUCT_BUILD);
            if (!PRODUCT_BUILD) {
                sb.withStartingCSV(operatorEnum.getStartingCSV());
            }
        }
        else {
            sb = ApicurioSubscriptionBuilder.base().withConfig(PRODUCT_BUILD);
        }

        sb.withChannel(operatorEnum.getSubscriptionUpdateChannel())
                .withNamespace(namespace);

        Subscription subscription = sb.build();
        ocp.operatorHub().subscriptions().inNamespace(namespace).createOrReplace(subscription);
        approveInstallPlan(ocp, subscription.getMetadata().getName(), namespace);
        utils.waitForOperatorDeploymentExists(namespace, operatorEnum);
    }

    public static void approveInstallPlan(OpenShiftClient ocp, String subscriptionName, String namespace) {
        final OpenShiftUtils utils = new OpenShiftUtils(ocp);

        utils.waitForOperatorSubscriptionExists(namespace, subscriptionName);
        Subscription subscription = utils.subscriptionWithName(namespace, subscriptionName).orElse(null);

        String installPlanName = null;
        if (subscription != null
                && subscription.getStatus() != null
                && subscription.getStatus().getInstallPlanRef() != null) {
            installPlanName = subscription.getStatus().getInstallPlanRef().getName();
        }

        if (installPlanName == null) {
            LOGGER.error("No InstallPlan reference found for subscription: " + subscriptionName);
            return;
        }

        // Wait for the install plan to be created
        utils.waitForOperatorInstallPlanExists(namespace, installPlanName);

        // Retrieve the InstallPlan
        InstallPlan plan = ocp.operatorHub()
                .installPlans()
                .inNamespace(namespace)
                .withName(installPlanName)
                .get();

        if (plan == null) {
            LOGGER.error("InstallPlan '" + installPlanName + "' not found!");
            return;
        }

        plan.getSpec().setApproved(true);
        ocp.operatorHub()
                .installPlans()
                .inNamespace(namespace)
                .withName(installPlanName)
                .replace(plan);
        LOGGER.info("InstallPlan '" + installPlanName + "' approved for subscription: " + subscriptionName);
    }
}

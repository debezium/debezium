/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka.builders;

import io.debezium.testing.system.tools.fabric8.FabricBuilderWrapper;
import io.debezium.testing.system.tools.fabric8.OperatorSubscriptionBuilder;
import io.debezium.testing.system.tools.operatorutil.OpenshiftOperatorEnum;
import io.debezium.testing.system.tools.operatorutil.SubscriptionConstants;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpecBuilder;

/**
 * Builder for strimzi/amq-streams subscription object.
 *
 */
public class StrimziSubscriptionBuilder extends FabricBuilderWrapper<StrimziSubscriptionBuilder, SubscriptionBuilder, Subscription>
        implements OperatorSubscriptionBuilder {

    private static final String STRIMZI_OPERATOR_NAME = "strimzi-kafka-operator";
    private static final String AMQ_STREAMS_OPERATOR_NAME = "amq-streams";

    protected StrimziSubscriptionBuilder(SubscriptionBuilder builder) {
        super(builder);
    }

    public static StrimziSubscriptionBuilder base() {
        var builder = new SubscriptionBuilder().withKind(SubscriptionConstants.SUBSCRIPTION_KIND)
                .withApiVersion(SubscriptionConstants.SUBSCRIPTION_API_VERSION);
        return new StrimziSubscriptionBuilder(builder);
    }

    /**
     * Base config for Strimzi subscription
     * @return
     */
    public StrimziSubscriptionBuilder withCommunityConfig() {
        builder.withMetadata(new ObjectMetaBuilder()
                .withName(OpenshiftOperatorEnum.STRIMZI.getDeploymentNamePrefix())
                .build())
                .withSpec(new SubscriptionSpecBuilder()
                        .withName(STRIMZI_OPERATOR_NAME)
                        .withSource(SubscriptionConstants.COMMUNITY_OPERATORS_SOURCE)
                        .withSourceNamespace(SubscriptionConstants.OPENSHIFT_MARKETPLACE_SOURCE_NAMESPACE)
                        .withInstallPlanApproval(SubscriptionConstants.INSTALL_PLAN_APPROVAL)
                        .build())
                .build();
        return self();
    }

    /**
     * Base config for Amq streams subscription
     * @return
     */
    public StrimziSubscriptionBuilder withProductConfig() {
        builder.withMetadata(new ObjectMetaBuilder()
                .withName(OpenshiftOperatorEnum.STRIMZI.getDeploymentNamePrefix())
                .build())
                .withSpec(new SubscriptionSpecBuilder()
                        .withName(AMQ_STREAMS_OPERATOR_NAME)
                        .withSource(SubscriptionConstants.REDHAT_OPERATORS_SOURCE)
                        .withSourceNamespace(SubscriptionConstants.OPENSHIFT_MARKETPLACE_SOURCE_NAMESPACE)
                        .withInstallPlanApproval(SubscriptionConstants.INSTALL_PLAN_APPROVAL)
                        .build());
        return self();
    }

    public StrimziSubscriptionBuilder withNamespace(String namespace) {
        builder.editMetadata()
                .withNamespace(namespace)
                .endMetadata();
        return self();
    }

    public StrimziSubscriptionBuilder withChannel(String channel) {
        builder.editSpec()
                .withChannel(channel)
                .endSpec();
        return self();
    }

    @Override
    public Subscription build() {
        return builder.build();
    }
}

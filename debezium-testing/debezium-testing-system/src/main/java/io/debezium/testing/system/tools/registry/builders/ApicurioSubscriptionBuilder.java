/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.registry.builders;

import io.debezium.testing.system.tools.fabric8.FabricBuilderWrapper;
import io.debezium.testing.system.tools.fabric8.OperatorSubscriptionBuilder;
import io.debezium.testing.system.tools.operatorutil.OpenshiftOperatorEnum;
import io.debezium.testing.system.tools.operatorutil.SubscriptionConstants;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionSpecBuilder;

/**
 * Builder for apicurio/service-registry subscription object.
 *
 */
public class ApicurioSubscriptionBuilder extends FabricBuilderWrapper<ApicurioSubscriptionBuilder, SubscriptionBuilder, Subscription>
        implements OperatorSubscriptionBuilder {

    private static final String APICURIO_OPERATOR_NAME = "apicurio-registry";
    private static final String SERVICE_REGISTRY_OPERATOR_NAME = "service-registry-operator";

    protected ApicurioSubscriptionBuilder(SubscriptionBuilder builder) {
        super(builder);
    }

    public static ApicurioSubscriptionBuilder base() {
        var builder = new SubscriptionBuilder().withKind(SubscriptionConstants.SUBSCRIPTION_KIND)
                .withApiVersion(SubscriptionConstants.SUBSCRIPTION_API_VERSION);
        return new ApicurioSubscriptionBuilder(builder);
    }

    @Override
    public Subscription build() {
        return builder.build();
    }

    /**
     * Base config for Service registry subscription
     * @return
     */
    public ApicurioSubscriptionBuilder withProductConfig() {
        builder.withMetadata(new ObjectMetaBuilder()
                .withName(OpenshiftOperatorEnum.APICURIO.getDeploymentNamePrefix())
                .build())
                .withSpec(new SubscriptionSpecBuilder()
                        .withName(SERVICE_REGISTRY_OPERATOR_NAME)
                        .withSource(SubscriptionConstants.REDHAT_OPERATORS_SOURCE)
                        .withSourceNamespace(SubscriptionConstants.OPENSHIFT_MARKETPLACE_SOURCE_NAMESPACE)
                        .withInstallPlanApproval(SubscriptionConstants.INSTALL_PLAN_APPROVAL)
                        .build())
                .build();
        return self();
    }

    /**
     * Base config for Apicurio subscription
     * @return
     */
    public ApicurioSubscriptionBuilder withCommunityConfig() {
        builder.withMetadata(new ObjectMetaBuilder()
                .withName(OpenshiftOperatorEnum.APICURIO.getDeploymentNamePrefix())
                .build())
                .withSpec(new SubscriptionSpecBuilder()
                        .withName(APICURIO_OPERATOR_NAME)
                        .withSource(SubscriptionConstants.COMMUNITY_OPERATORS_SOURCE)
                        .withSourceNamespace(SubscriptionConstants.OPENSHIFT_MARKETPLACE_SOURCE_NAMESPACE)
                        .withInstallPlanApproval(SubscriptionConstants.INSTALL_PLAN_APPROVAL)
                        .build())
                .build();
        return self();
    }

    public ApicurioSubscriptionBuilder withNamespace(String namespace) {
        builder.editMetadata()
                .withNamespace(namespace)
                .endMetadata();
        return self();
    }

    public ApicurioSubscriptionBuilder withChannel(String channel) {
        builder.editSpec()
                .withChannel(channel)
                .endSpec();
        return self();
    }
}

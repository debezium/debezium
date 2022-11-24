/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.fabric8;

import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;

public interface OperatorSubscriptionBuilder {
    Subscription build();

    OperatorSubscriptionBuilder withCommunityConfig();

    OperatorSubscriptionBuilder withProductConfig();

    OperatorSubscriptionBuilder withNamespace(String namespace);

    OperatorSubscriptionBuilder withChannel(String namespace);
}

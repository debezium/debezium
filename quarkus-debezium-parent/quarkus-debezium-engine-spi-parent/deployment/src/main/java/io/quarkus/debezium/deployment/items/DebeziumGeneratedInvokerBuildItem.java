/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.items;

import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.builder.item.MultiBuildItem;

public final class DebeziumGeneratedInvokerBuildItem extends MultiBuildItem {
    private final String generatedClassName;
    private final BeanInfo mediator;

    public DebeziumGeneratedInvokerBuildItem(String generatedClassName, BeanInfo mediator) {
        this.generatedClassName = generatedClassName;
        this.mediator = mediator;
    }

    public String getGeneratedClassName() {
        return generatedClassName;
    }

    public BeanInfo getMediator() {
        return mediator;
    }
}

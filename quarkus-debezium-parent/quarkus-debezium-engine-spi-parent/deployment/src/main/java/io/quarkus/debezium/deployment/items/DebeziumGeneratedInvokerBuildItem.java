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
    private final BeanInfo delegate;

    public DebeziumGeneratedInvokerBuildItem(String generatedClassName, BeanInfo delegate) {
        this.generatedClassName = generatedClassName;
        this.delegate = delegate;
    }

    public String getGeneratedClassName() {
        return generatedClassName;
    }

    public BeanInfo getDelegate() {
        return delegate;
    }
}

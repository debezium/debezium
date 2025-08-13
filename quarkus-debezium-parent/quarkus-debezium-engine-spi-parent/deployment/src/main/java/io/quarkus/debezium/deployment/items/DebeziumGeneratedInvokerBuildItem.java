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
    private final String id;
    private final Class<?> clazz;

    public DebeziumGeneratedInvokerBuildItem(String generatedClassName, BeanInfo mediator, String id, Class<?> clazz) {
        this.generatedClassName = generatedClassName;
        this.mediator = mediator;
        this.id = id;
        this.clazz = clazz;
    }

    public String getGeneratedClassName() {
        return generatedClassName;
    }

    public BeanInfo getMediator() {
        return mediator;
    }

    public String getId() {
        return id;
    }

    public Class<?> getClazz() {
        return clazz;
    }
}

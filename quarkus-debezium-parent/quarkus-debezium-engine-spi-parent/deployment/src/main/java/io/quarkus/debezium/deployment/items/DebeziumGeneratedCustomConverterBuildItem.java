/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.items;

import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.builder.item.MultiBuildItem;

public final class DebeziumGeneratedCustomConverterBuildItem extends MultiBuildItem {
    private final String generatedClassName;
    private final BeanInfo binder;
    private final String id;
    private final BeanInfo filter;

    public DebeziumGeneratedCustomConverterBuildItem(String generatedClassName, BeanInfo binder, String id, BeanInfo filter) {
        this.generatedClassName = generatedClassName;
        this.binder = binder;
        this.id = id;
        this.filter = filter;
    }

    public String getGeneratedClassName() {
        return generatedClassName;
    }

    public BeanInfo getBinder() {
        return binder;
    }

    public String getId() {
        return id;
    }

    public BeanInfo getFilter() {
        return filter;
    }
}

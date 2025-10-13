/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.items;

import io.quarkus.builder.item.MultiBuildItem;

/**
 * Contains the name of the extension
 */
public final class DebeziumExtensionNameBuildItem extends MultiBuildItem {
    private final String name;

    public DebeziumExtensionNameBuildItem(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.mongodb.deployment;

import io.quarkus.builder.item.MultiBuildItem;

public final class DebeziumDatasourceBuildItem extends MultiBuildItem {
    private final String name;

    public DebeziumDatasourceBuildItem(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public boolean isDefault() {
        return name.equals("<default>");
    }
}

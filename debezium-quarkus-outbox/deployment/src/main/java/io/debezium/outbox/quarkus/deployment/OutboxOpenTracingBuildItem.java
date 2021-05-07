/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import io.quarkus.builder.item.SimpleBuildItem;

/**
 * A build item that represents data relevant to OpenTracing dependency.
 *
 * @author Anisha Mohanty
 */
public final class OutboxOpenTracingBuildItem extends SimpleBuildItem {

    private final boolean enabled;

    public OutboxOpenTracingBuildItem(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.deployment;

import org.jboss.jandex.Type;

import io.quarkus.builder.item.SimpleBuildItem;

/**
 * A build item that represents data relevant to the OutboxEvent entity mapping.
 *
 * @author Chris Cranford
 */
public final class OutboxEventEntityBuildItem extends SimpleBuildItem {
    private final Type aggregateIdType;
    private final Type payloadType;

    public OutboxEventEntityBuildItem(Type aggregateIdType, Type payloadType) {
        this.aggregateIdType = aggregateIdType;
        this.payloadType = payloadType;
    }

    public Type getAggregateIdType() {
        return aggregateIdType;
    }

    public Type getPayloadType() {
        return payloadType;
    }
}

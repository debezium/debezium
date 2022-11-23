/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.debezium.outbox.quarkus.DebeziumCustomCodec;
import io.debezium.outbox.quarkus.ExportedEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mutiny.core.eventbus.EventBus;

@ApplicationScoped
public class DebeziumOutboxHandler {

    @Inject
    EventBus bus;
    DebeziumCustomCodec myCodec = new DebeziumCustomCodec();
    DeliveryOptions options = new DeliveryOptions().setCodecName(myCodec.name());

    public Uni<Object> persistToOutbox(ExportedEvent<?, ?> incomingEvent) {
        return bus.<Object> request("debezium-outbox", incomingEvent, options)
                .onItem().transform(message -> message.body());
    }
}

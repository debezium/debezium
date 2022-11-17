/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.jboss.logging.Logger;

import io.debezium.outbox.quarkus.DebeziumCustomCodec;
import io.debezium.outbox.quarkus.ExportedEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mutiny.core.eventbus.EventBus;

@ApplicationScoped
public class DebeziumOutboxHandler {
    private static final Logger LOGGER = Logger.getLogger(DebeziumOutboxHandler.class);
    @Inject
    EventBus bus;
    DebeziumCustomCodec myCodec = new DebeziumCustomCodec();
    DeliveryOptions options = new DeliveryOptions().setCodecName(myCodec.name());

    public Uni<Object> persistToOutbox(ExportedEvent<?, ?> incomingEvent) {
        LOGGER.infof("starting on thread: " + Thread.currentThread().getName());
        return bus.<Object> request("debezium-outbox", incomingEvent, options)
                .onItem().transform(message -> message.body()).invoke(() -> LOGGER.infof("finishing on thread: " + Thread.currentThread().getName()));
    }
}

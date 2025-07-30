/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.CapturingEvent;
import io.quarkus.debezium.engine.OperationMapper;
import io.quarkus.debezium.engine.capture.CapturingInvoker;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;
import io.quarkus.debezium.engine.deserializer.CapturingEventDeserializerRegistry;

public class SourceRecordEventProducer {

    private final CapturingInvokerRegistry<CapturingEvent<?>> capturingEventRegistry;
    private final CapturingEventDeserializerRegistry<SourceRecord> capturingEventDeserializerRegistry;
    private final CapturingInvokerRegistry<Object> capturingObjectInvokerRegistry;

    @Inject
    public SourceRecordEventProducer(CapturingInvokerRegistry<CapturingEvent<?>> capturingEventRegistry,
                                     CapturingEventDeserializerRegistry<SourceRecord> capturingEventDeserializerRegistry,
                                     CapturingInvokerRegistry<Object> capturingObjectInvokerRegistry) {
        this.capturingEventRegistry = capturingEventRegistry;
        this.capturingEventDeserializerRegistry = capturingEventDeserializerRegistry;
        this.capturingObjectInvokerRegistry = capturingObjectInvokerRegistry;
    }

    @Produces
    @Singleton
    public SourceRecordEventConsumer produce() {
        return event -> {
            CapturingEvent<SourceRecord> capturingEvent = OperationMapper.from(event);

            var deserializer = capturingEventDeserializerRegistry.get(capturingEvent.destination());
            CapturingInvoker<Object> objectCapturingInvoker = capturingObjectInvokerRegistry.get(capturingEvent.destination());

            if (deserializer != null && objectCapturingInvoker != null) {
                objectCapturingInvoker.capture(deserializer.deserialize(capturingEvent).record());
                return;
            }

            if (deserializer != null) {
                capturingEventRegistry.get(capturingEvent.destination()).capture(deserializer.deserialize(capturingEvent));
                return;
            }

            capturingEventRegistry.get(capturingEvent.destination()).capture(capturingEvent);
        };
    }
}

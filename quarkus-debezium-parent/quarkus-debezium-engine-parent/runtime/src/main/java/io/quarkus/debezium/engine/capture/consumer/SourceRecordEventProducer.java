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

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.CapturingEvent;
import io.quarkus.debezium.engine.OperationMapper;
import io.quarkus.debezium.engine.capture.CapturingEventInvoker;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;
import io.quarkus.debezium.engine.deserializer.CapturingEventDeserializerRegistry;

public class SourceRecordEventProducer {

    private final CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> recordChangeEventRegistry;
    private final CapturingInvokerRegistry<CapturingEvent<?>> capturingEventRegistry;
    private final CapturingEventDeserializerRegistry<SourceRecord> capturingEventDeserializerRegistry;

    @Inject
    public SourceRecordEventProducer(CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> recordChangeEventRegistry,
                                     CapturingInvokerRegistry<CapturingEvent<?>> capturingEventRegistry,
                                     CapturingEventDeserializerRegistry<SourceRecord> capturingEventDeserializerRegistry) {
        this.recordChangeEventRegistry = recordChangeEventRegistry;
        this.capturingEventRegistry = capturingEventRegistry;
        this.capturingEventDeserializerRegistry = capturingEventDeserializerRegistry;
    }

    @Produces
    @Singleton
    public SourceRecordEventConsumer produce() {
        return event -> {
            CapturingEvent<SourceRecord> capturingEvent = OperationMapper.from(event);

            var deserializer = capturingEventDeserializerRegistry.get(capturingEvent.destination());

            if (deserializer != null) {
                capturingEventRegistry.get(capturingEvent).capture(deserializer.deserialize(capturingEvent));
                return;
            }

            var invoker = capturingEventRegistry.get(capturingEvent);

            if (invoker != null) {
                invoker.capture(capturingEvent);
                return;
            }

            new CapturingEventInvoker() {
                @Override
                public String destination() {
                    return capturingEvent.destination();
                }

                @Override
                public void capture(CapturingEvent<SourceRecord> innerEvent) {
                    recordChangeEventRegistry.get(capturingEvent::record).capture(innerEvent::record);
                }
            }.capture(capturingEvent);
        };
    }
}

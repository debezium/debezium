/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture.consumer;

import java.util.Objects;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.RecordChangeEvent;
import io.debezium.runtime.CapturingEvent;
import io.quarkus.debezium.engine.OperationMapper;
import io.quarkus.debezium.engine.capture.CapturingEventInvoker;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;

public class SourceRecordEventProducer {

    private final CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> recordChangeEventRegistry;
    private final CapturingInvokerRegistry<CapturingEvent<SourceRecord>> capturingEventRegistry;

    @Inject
    public SourceRecordEventProducer(CapturingInvokerRegistry<RecordChangeEvent<SourceRecord>> recordChangeEventRegistry,
                                     CapturingInvokerRegistry<CapturingEvent<SourceRecord>> capturingEventRegistry) {
        this.recordChangeEventRegistry = recordChangeEventRegistry;
        this.capturingEventRegistry = capturingEventRegistry;
    }

    @Produces
    @Singleton
    public SourceRecordEventConsumer produce() {
        return event -> {
            var capturingEvent = OperationMapper.from(event);

            var invoker = Objects.requireNonNullElseGet(capturingEventRegistry.get(capturingEvent), () -> new CapturingEventInvoker() {
                @Override
                public String destination() {
                    return capturingEvent.destination();
                }

                @Override
                public void capture(CapturingEvent<SourceRecord> innerEvent) {
                    recordChangeEventRegistry.get(capturingEvent::record).capture(innerEvent::record);
                }
            });

            invoker.capture(capturingEvent);
        };
    }
}

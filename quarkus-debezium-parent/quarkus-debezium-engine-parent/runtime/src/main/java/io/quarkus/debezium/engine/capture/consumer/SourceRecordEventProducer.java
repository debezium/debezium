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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.runtime.CapturingEvent;
import io.quarkus.debezium.engine.OperationMapper;
import io.quarkus.debezium.engine.capture.CapturingInvoker;
import io.quarkus.debezium.engine.capture.CapturingInvokerRegistry;
import io.quarkus.debezium.engine.deserializer.CapturingEventDeserializerRegistry;

public class SourceRecordEventProducer {

    private final CapturingInvokerRegistry<CapturingEvent<?>> capturingEventRegistry;
    private final CapturingInvokerRegistry<Object> capturingObjectInvokerRegistry;
    private final CapturingEventDeserializerRegistry<SourceRecord> capturingEventDeserializerRegistry;

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
        return new SourceRecordEventConsumer() {
            private final Logger logger = LoggerFactory.getLogger(SourceRecordEventConsumer.class);

            @Override
            public void accept(ChangeEvent<SourceRecord, SourceRecord> event) {
                CapturingEvent<SourceRecord> capturingEvent = new OperationMapper("default").from(event);

                var deserializer = capturingEventDeserializerRegistry.get(capturingEvent.destination());
                CapturingInvoker<Object> objectCapturingInvoker = capturingObjectInvokerRegistry.get(capturingEvent);

                if (deserializer != null && objectCapturingInvoker != null) {
                    objectCapturingInvoker.capture(deserializer.deserialize(capturingEvent).record());
                    return;
                }

                var invoker = capturingEventRegistry.get(capturingEvent);

                if (invoker == null) {
                    logger.debug("method annotated with @Capturing not found for destination: {}", capturingEvent.destination());
                    return;
                }

                if (deserializer != null) {
                    invoker.capture(deserializer.deserialize(capturingEvent));
                    return;
                }

                invoker.capture(capturingEvent);
            }
        };
    }
}

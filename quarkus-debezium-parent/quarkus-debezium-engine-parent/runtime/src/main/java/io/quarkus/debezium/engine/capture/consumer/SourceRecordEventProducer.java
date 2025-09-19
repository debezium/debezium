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
    public SourceRecordConsumerHandler produce() {
        return capturingGroup -> new SourceRecordEventConsumer() {
            private final Logger logger = LoggerFactory.getLogger(SourceRecordEventConsumer.class);

            @Override
            public void accept(ChangeEvent<SourceRecord, SourceRecord> event) {
                logger.trace("receiving event {} with group {}", event.destination(), capturingGroup.id());
                CapturingEvent<SourceRecord> capturingEvent = new OperationMapper(capturingGroup.id()).from(event);

                var deserializer = capturingEventDeserializerRegistry.get(capturingEvent.destination());
                CapturingInvoker<Object> objectCapturingInvoker = capturingObjectInvokerRegistry.get(capturingEvent);

                if (deserializer != null && objectCapturingInvoker != null) {
                    logger.trace("method annotated with @Capturing with object mapping found for destination: {}", capturingEvent.destination());
                    objectCapturingInvoker.capture(deserializer.deserialize(capturingEvent).record());
                    return;
                }

                var invoker = capturingEventRegistry.get(capturingEvent);

                if (invoker == null) {
                    logger.trace("method annotated with @Capturing not found for destination: {}", capturingEvent.destination());
                    return;
                }

                if (deserializer != null) {
                    logger.trace("deserializer found for destination: {}", capturingEvent.destination());
                    invoker.capture(deserializer.deserialize(capturingEvent));
                    return;
                }

                logger.trace("deserializer not found, using default invoker for: {}", capturingEvent.destination());
                invoker.capture(capturingEvent);
            }
        };
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.runtime.CapturingEvent;

public class CapturingEventInvokerRegistryProducer implements CapturingInvokerRegistryProducer<CapturingEvent<SourceRecord>> {

    @Inject
    Instance<CapturingEventInvoker> invokers;

    @Override
    @Produces
    @Singleton
    public CapturingInvokerRegistry<CapturingEvent<SourceRecord>> produce() {
        Map<String, CapturingEventInvoker> invokers = this.invokers
                .stream()
                .collect(Collectors.toMap(CapturingEventInvoker::destination, Function.identity()));

        return event -> invokers.get(event.destination());
    }
}

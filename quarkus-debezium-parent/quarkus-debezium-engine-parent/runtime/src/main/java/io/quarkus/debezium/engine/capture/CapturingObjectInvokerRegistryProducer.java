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

public class CapturingObjectInvokerRegistryProducer {
    private final CapturingInvokerValidator<CapturingObjectInvoker> validator = new CapturingInvokerValidator<>();

    @Inject
    Instance<CapturingObjectInvoker> invokers;

    @Produces
    @Singleton
    public CapturingInvokerRegistry<Object> produce() {
        validator.validate(this.invokers
                .stream()
                .toList());

        Map<String, CapturingObjectInvoker> invokers = this.invokers
                .stream()
                .collect(Collectors.toMap(CapturingInvoker::generateKey, Function.identity()));

        return event -> invokers.get(CapturingInvoker.getKey(event));
    }

}

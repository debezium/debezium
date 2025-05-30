/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import io.quarkus.debezium.engine.FullyQualifiedTableNameResolver;

public class CapturingHandlerProducer {

    @Inject
    Instance<CapturingInvoker> invokers;

    @Inject
    FullyQualifiedTableNameResolver resolver;

    @Produces
    @Singleton
    public DebeziumCapturingHandler produce() {
        CapturingInvokerRegistry capturingInvokerRegistry = new CapturingInvokerRegistry(invokers
                .stream()
                .collect(Collectors.toMap(CapturingInvoker::getFullyQualifiedTableName, Function.identity())));

        return new DefaultCapturingHandler(capturingInvokerRegistry, resolver);
    }

}

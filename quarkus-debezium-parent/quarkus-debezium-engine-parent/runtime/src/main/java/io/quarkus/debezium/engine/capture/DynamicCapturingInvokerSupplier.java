/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.capture;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import jakarta.enterprise.invoke.Invoker;

import io.quarkus.arc.Arc;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DynamicCapturingInvokerSupplier {

    public static final String BASE_NAME = "invoker";

    public Supplier<CapturingInvoker> createInvoker(Class<?> mediatorClazz, Class<? extends Invoker> invokerClazz) {
        try {
            Object mediator = Arc.container().instance(mediatorClazz).get();
            CapturingInvoker instance = (CapturingInvoker) invokerClazz
                    .getDeclaredConstructor(Object.class)
                    .newInstance(mediator);

            return () -> instance;

        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}

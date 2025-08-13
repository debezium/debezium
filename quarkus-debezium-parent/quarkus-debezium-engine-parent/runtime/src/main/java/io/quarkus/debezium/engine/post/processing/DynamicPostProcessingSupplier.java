/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.post.processing;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import io.debezium.processors.spi.PostProcessor;
import io.quarkus.arc.Arc;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DynamicPostProcessingSupplier {

    public static final String BASE_NAME = "postProcessing";

    public Supplier<PostProcessor> createPostProcessor(Class<?> mediatorClazz, Class<? extends PostProcessor> invokerClazz) {
        try {
            Object mediator = Arc.container().instance(mediatorClazz).get();
            PostProcessor instance = invokerClazz
                    .getDeclaredConstructor(Object.class)
                    .newInstance(mediator);

            return () -> instance;

        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}

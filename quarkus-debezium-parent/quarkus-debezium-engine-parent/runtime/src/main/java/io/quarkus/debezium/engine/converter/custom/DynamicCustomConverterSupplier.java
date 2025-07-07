/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.converter.custom;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import io.quarkus.arc.Arc;
import io.quarkus.debezium.engine.relational.converter.QuarkusCustomConverter;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class DynamicCustomConverterSupplier {

    public static String BASE_NAME = "customConverter";

    public Supplier<QuarkusCustomConverter> createQuarkusCustomConverter(Class<?> binderClazz, Class<?> filterClazz,
                                                                         Class<? extends QuarkusCustomConverter> converterClazz) {
        try {
            Object binder = Arc.container().instance(binderClazz).get();

            if (filterClazz != null) {
                Object filter = Arc.container().instance(filterClazz).get();

                var instance = converterClazz
                        .getDeclaredConstructor(Object.class, Object.class)
                        .newInstance(binder, filter);

                return () -> instance;
            }

            var instance = converterClazz
                    .getDeclaredConstructor(Object.class)
                    .newInstance(binder);

            return () -> instance;

        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

/**
 * Reflection utility methods.
 */
public class Reflections {

    public static <T> T invokeMethodWithFallbackName(Object target, String newMethodName, String oldMethodName, Class<T> returnType) {
        Objects.requireNonNull(target, "The method owner object must not be null");
        Objects.requireNonNull(newMethodName, "The new method name must not be null");
        Objects.requireNonNull(oldMethodName, "The old method name must not be null");
        Objects.requireNonNull(returnType, "The return type must not be null");

        final Class<?> type = target.getClass();

        for (String methodName : List.of(newMethodName, oldMethodName)) {
            try {
                Method matchingMethod = type.getMethod(methodName);
                matchingMethod.setAccessible(true);
                return returnType.cast(matchingMethod.invoke(target));
            }
            catch (NoSuchMethodException e) {
                // Try fallback name
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("Could not invoke method '%s' on %s".formatted(methodName, type.getName()), e);
            }
        }

        throw new IllegalStateException(
                "Unable to invoke no-arg methods '%s' or '%s' on %s".formatted(newMethodName, oldMethodName, type.getName()));
    }

    private Reflections() {
    }
}

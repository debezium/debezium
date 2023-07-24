/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.tracing;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterceptorVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterceptorVersion.class);

    private final String className;
    private Class<?> interceptorClass;

    public InterceptorVersion(String className) {
        this.className = className;
        try {
            this.interceptorClass = Class.forName(className);
            LOGGER.debug("Class {} found", className);

        }
        catch (ClassNotFoundException e) {
            LOGGER.debug("Class {} not found", className);
            this.interceptorClass = null;
        }
    }

    public Object createInstance() {
        if (interceptorClass == null) {
            return null;
        }
        try {
            return interceptorClass.getDeclaredConstructor().newInstance();
        }
        catch (Exception e) {
            LOGGER.error("Unable to instantiate {}", className, e);
            return null;
        }
    }

    public Method getMethod(String name, Class<?>... parameterTypes) {
        if (interceptorClass == null) {
            return null;
        }
        try {
            return interceptorClass.getMethod(name, parameterTypes);
        }
        catch (Exception e) {
            LOGGER.error("Unable to get method {} from {}", name, className, e);
            return null;
        }
    }
}

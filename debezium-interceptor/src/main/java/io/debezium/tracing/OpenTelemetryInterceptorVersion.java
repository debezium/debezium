/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.tracing;

import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a specific version of the OpenTelemetry interceptor by its class name.
 * <p>
 * An instance of this class attempts to dynamically load the OpenTelemetry interceptor class from the provided class name.
 * If the class is present in the classpath (meaning that the relevant version of OpenTelemetry is being used),
 * it gets loaded and methods can be retrieved from it for further invocation.
 * If the class is not present, all operations gracefully degrade to return null, indicating that the version is not available.
 * <p>
 * This dynamic approach allows the Debezium Kafka Connector to interact with different versions of the OpenTelemetry interceptor,
 * without having a direct compile-time dependency. It provides a level of abstraction over the changes in the OpenTelemetry interceptor's
 * class name and package between different versions of OpenTelemetry.
 */
public class OpenTelemetryInterceptorVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryInterceptorVersion.class);

    private final String className;
    private Class<?> interceptorClass;

    public OpenTelemetryInterceptorVersion(String className) {
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
            LOGGER.debug("Unable to instantiate {}", className, e);
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
            LOGGER.debug("Unable to get method {} from {}", name, className, e);
            return null;
        }
    }
}

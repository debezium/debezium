/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import io.quarkus.arc.processor.BeanInfo;

/**
 * Generator interface for different {@link io.quarkus.debezium.engine.capture.CapturingInvoker}
 */
public interface CapturingInvokerGenerator {

    /**
     * Given the {@link Type} defined in the annotation returns if it's compatible with this generator
     * @param type
     */
    default boolean isCompatible(Type type) {
        return false;
    }

    GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo);
}

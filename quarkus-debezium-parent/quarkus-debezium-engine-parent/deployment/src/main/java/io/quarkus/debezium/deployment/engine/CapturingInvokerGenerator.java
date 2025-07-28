/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import io.quarkus.arc.processor.BeanInfo;

public interface CapturingInvokerGenerator {

    default boolean isCompatible(Type type) {
        return false;
    }

    GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo);
}

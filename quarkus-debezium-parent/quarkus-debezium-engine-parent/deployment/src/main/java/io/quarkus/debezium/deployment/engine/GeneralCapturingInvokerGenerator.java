/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;

import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.gizmo.ClassOutput;

public class GeneralCapturingInvokerGenerator implements CapturingInvokerGenerator {

    private final ClassOutput output;

    public GeneralCapturingInvokerGenerator(ClassOutput classOutput) {
        this.output = classOutput;
    }

    @Override
    public boolean isCompatible(Type type) {
        return !(type instanceof ParameterizedType);
    }

    @Override
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        return null;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import io.quarkus.arc.processor.BeanInfo;

public class DefaultCapturingInvokerGenerator implements CapturingInvokerGenerator {
    private final Map<Type, CapturingInvokerGenerator> generatorMap;

    public DefaultCapturingInvokerGenerator(CapturingInvokerGenerator... generators) {
        generatorMap = Arrays.stream(generators)
                .collect(Collectors.toMap(CapturingInvokerGenerator::type, Function.identity()));
    }

    @Override
    public Type type() {
        return null;
    }

    @Override
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        return generatorMap.get(methodInfo.parameters().getFirst().type())
                .generate(methodInfo, beanInfo);
    }
}

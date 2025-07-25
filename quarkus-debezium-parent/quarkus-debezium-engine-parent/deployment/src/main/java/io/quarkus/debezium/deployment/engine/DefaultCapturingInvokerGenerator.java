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

import org.apache.kafka.connect.source.SourceRecord;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;

import io.debezium.runtime.CapturingEvent;
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
        return generatorMap
                .getOrDefault(methodInfo.parameters().getFirst().type(),
                        generatorMap.get(ParameterizedType.create(CapturingEvent.class, Type.create(SourceRecord.class))))
                .generate(methodInfo, beanInfo);
    }
}

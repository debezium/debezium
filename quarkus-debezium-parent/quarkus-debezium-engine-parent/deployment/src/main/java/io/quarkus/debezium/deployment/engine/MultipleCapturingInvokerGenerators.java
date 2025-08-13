/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.util.Arrays;
import java.util.List;

import org.jboss.jandex.MethodInfo;

import io.quarkus.arc.processor.BeanInfo;

public class MultipleCapturingInvokerGenerators implements CapturingInvokerGenerator {
    private final CapturingInvokerGenerator defaultGenerator;
    private final List<CapturingInvokerGenerator> generators;

    public MultipleCapturingInvokerGenerators(CapturingInvokerGenerator defaultGenerator,
                                              CapturingInvokerGenerator... generators) {
        this.defaultGenerator = defaultGenerator;
        this.generators = Arrays.stream(generators).toList();
    }

    @Override
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        return generators
                .stream()
                .filter(generator -> generator.isCompatible(methodInfo.parameters().getFirst().type()))
                .findFirst()
                .map(generator -> generator.generate(methodInfo, beanInfo))
                .orElseGet(() -> defaultGenerator.generate(methodInfo, beanInfo));
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.data.Struct;
import org.jboss.jandex.MethodInfo;

import io.debezium.processors.spi.PostProcessor;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.runtime.util.HashUtil;

public class PostProcessorGenerator {
    private final ClassOutput output;

    public PostProcessorGenerator(ClassOutput output) {
        this.output = output;
    }

    /**
     * it generates concrete classes based on the PostProcessor interface using gizmo:
     * <p>
     * public class GeneratedPostProcessor implements PostProcessor {
     *     private final Object beanInstance;
     *
     *     void configure(Map<String, ?> properties) {
     *         // empty configuration can be taken from CDI
     *     }
     *
     *     void apply(Object key, Struct value) {
     *         beanInstance.method(key, value);
     *     }
     *
     *     void close() {
     *         // resources can be managed outside
     *     }
     * }
     * @param methodInfo
     * @param beanInfo
     * @return
     */
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        String name = generateClassName(beanInfo, methodInfo);
        try (ClassCreator invoker = ClassCreator.builder()
                .classOutput(this.output)
                .className(name)
                .interfaces(PostProcessor.class)
                .build()) {

            FieldDescriptor beanInstanceField = invoker.getFieldCreator("beanInstance", methodInfo
                    .declaringClass()
                    .name()
                    .toString())
                    .setModifiers(Modifier.PRIVATE)
                    .getFieldDescriptor();

            try (MethodCreator constructor = invoker.getMethodCreator("<init>", void.class, Object.class)) {
                constructor.setModifiers(Modifier.PUBLIC);
                constructor.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructor.getThis());
                ResultHandle constructorThis = constructor.getThis();
                ResultHandle beanInstance = constructor.getMethodParam(0);
                constructor.writeInstanceField(beanInstanceField, constructorThis, beanInstance);
                constructor.returnValue(null);
            }

            try (MethodCreator apply = invoker.getMethodCreator("apply", void.class, Object.class, Struct.class)) {
                ResultHandle applyThis = apply.getThis();
                ResultHandle delegate = apply.readInstanceField(beanInstanceField, applyThis);
                ResultHandle eventObject = apply.getMethodParam(0);
                ResultHandle eventStruct = apply.getMethodParam(1);

                MethodDescriptor methodDescriptor = MethodDescriptor.ofMethod(
                        methodInfo.declaringClass().toString(),
                        methodInfo.name(),
                        "V",
                        methodInfo.parameterType(0).name().toString(),
                        methodInfo.parameterType(1).name().toString());

                apply.invokeVirtualMethod(methodDescriptor, delegate, eventObject, eventStruct);
                apply.returnVoid();
            }

            try (MethodCreator configure = invoker.getMethodCreator("configure", void.class, Map.class)) {
                configure.setModifiers(Modifier.PUBLIC);
                configure.returnVoid();
            }

            try (MethodCreator close = invoker.getMethodCreator("close", void.class)) {
                close.setModifiers(Modifier.PUBLIC);
                close.returnVoid();
            }
        }

        return new GeneratedClassMetaData(UUID.randomUUID(), name.replace('/', '.'), beanInfo);
    }

    private String generateClassName(BeanInfo bean, MethodInfo methodInfo) {
        return DotNames.internalPackageNameWithTrailingSlash(bean.getImplClazz().name())
                + DotNames.simpleName(bean.getImplClazz().name())
                + "_DebeziumPostProcessor" + "_"
                + methodInfo.name() + "_"
                + HashUtil.sha1(methodInfo.name() + "_" + methodInfo.returnType().name().toString());
    }
}

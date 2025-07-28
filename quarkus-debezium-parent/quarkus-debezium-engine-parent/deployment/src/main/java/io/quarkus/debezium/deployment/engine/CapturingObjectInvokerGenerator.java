/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.lang.reflect.Modifier;
import java.util.Optional;
import java.util.UUID;

import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;

import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.deployment.dotnames.DebeziumDotNames;
import io.quarkus.debezium.engine.capture.CapturingObjectInvoker;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.runtime.util.HashUtil;

public class CapturingObjectInvokerGenerator implements CapturingInvokerGenerator {

    private final ClassOutput output;

    public CapturingObjectInvokerGenerator(ClassOutput classOutput) {
        this.output = classOutput;
    }

    @Override
    public boolean isCompatible(Type type) {
        return !(type instanceof ParameterizedType);
    }

    /**
     * it generates concrete classes based on the {@link io.quarkus.debezium.engine.capture.CapturingObjectInvoker} interface using gizmo:
     * <p>
     * public class GeneratedCapturingInvoker {
     * private final Object beanInstance;
     * <p>
     * void capture(Object event) {
     * beanInstance.method(event);
     * }
     * <p>
     * }
     *
     * @param methodInfo
     * @param beanInfo
     * @return
     */
    @Override
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        String name = generateClassName(beanInfo, methodInfo);

        try (ClassCreator invoker = ClassCreator.builder()
                .classOutput(this.output)
                .className(name)
                .interfaces(CapturingObjectInvoker.class)
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

            try (MethodCreator capture = invoker.getMethodCreator("capture", void.class, Object.class)) {
                ResultHandle captureThis = capture.getThis();
                ResultHandle delegate = capture.readInstanceField(beanInstanceField, captureThis);
                ResultHandle event = capture.getMethodParam(0);

                MethodDescriptor methodDescriptor = MethodDescriptor.ofMethod(
                        methodInfo.declaringClass().toString(),
                        methodInfo.name(),
                        "V",
                        methodInfo.parameterType(0).name().toString());

                capture.invokeVirtualMethod(methodDescriptor, delegate, event);
                capture.returnVoid();
            }

            try (MethodCreator destination = invoker.getMethodCreator("destination", String.class)) {
                Optional.ofNullable(methodInfo
                        .annotation(DebeziumDotNames.CAPTURING)
                        .value("destination"))
                        .map(AnnotationValue::asString)
                        .ifPresentOrElse(s -> destination.returnValue(destination.load(s)),
                                () -> {
                                    throw new RuntimeException("trying to capture events without defining a destination for class " + methodInfo.declaringClass());
                                });
            }

            return new GeneratedClassMetaData(UUID.randomUUID(), name.replace('/', '.'), beanInfo, CapturingObjectInvoker.class);
        }
    }

    private String generateClassName(BeanInfo bean, MethodInfo methodInfo) {
        return DotNames.internalPackageNameWithTrailingSlash(bean.getImplClazz().name())
                + DotNames.simpleName(bean.getImplClazz().name())
                + "_DebeziumInvoker" + "_"
                + methodInfo.name() + "_"
                + HashUtil.sha1(methodInfo.name() + "_" + methodInfo.returnType().name().toString());
    }

}

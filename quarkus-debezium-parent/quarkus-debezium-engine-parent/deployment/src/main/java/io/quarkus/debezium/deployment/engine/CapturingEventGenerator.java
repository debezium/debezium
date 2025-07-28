/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.lang.reflect.Modifier;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.connect.source.SourceRecord;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.deployment.dotnames.DebeziumDotNames;
import io.quarkus.debezium.engine.capture.CapturingEventInvoker;
import io.quarkus.gizmo.*;
import io.quarkus.runtime.util.HashUtil;

public class CapturingEventGenerator implements CapturingInvokerGenerator {

    private final ClassOutput output;

    public CapturingEventGenerator(ClassOutput output) {
        this.output = output;
    }

    @Override
    public boolean isCompatible(Type type) {
        return ParameterizedType.create(CapturingEvent.class, Type.create(SourceRecord.class)).equals(type);
    }

    /**
     * it generates concrete classes based on the {@link io.quarkus.debezium.engine.capture.CapturingEventInvoker} interface using gizmo:
     * <p>
     * public class GeneratedCapturingInvoker {
     *     private final Object beanInstance;
     *
     *     void capture(CapturingEvent<SourceRecord> event) {
     *         beanInstance.method(event);
     *     }
     *
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
                .interfaces(CapturingEventInvoker.class)
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

            try (MethodCreator capture = invoker.getMethodCreator("capture", void.class, CapturingEvent.class)) {
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
                                () -> destination.returnValue(destination.load(Capturing.ALL)));
            }

            return new GeneratedClassMetaData(UUID.randomUUID(), name.replace('/', '.'), beanInfo, CapturingEventInvoker.class);
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

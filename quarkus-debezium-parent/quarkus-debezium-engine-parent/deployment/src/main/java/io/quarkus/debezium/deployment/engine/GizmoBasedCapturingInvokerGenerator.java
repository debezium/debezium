/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.deployment.engine;

import java.lang.reflect.Modifier;
import java.util.Optional;

import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.MethodInfo;

import io.debezium.runtime.Capturing;
import io.quarkus.debezium.deployment.dotnames.DebeziumDotNames;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;

/**
 * InvokerGenerator that creates Invokers with Gizmo
 */
interface GizmoBasedCapturingInvokerGenerator extends CapturingInvokerGenerator {

    default FieldDescriptor constructorWithObjectField(MethodInfo methodInfo, ClassCreator invoker, String fieldName) {
        FieldDescriptor beanInstanceField = invoker.getFieldCreator(fieldName, methodInfo
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
        return beanInstanceField;
    }

    default void createCaptureMethod(MethodInfo methodInfo, ClassCreator invoker, FieldDescriptor beanInstanceField, Class<?> clazz) {
        try (MethodCreator capture = invoker.getMethodCreator("capture", void.class, clazz)) {
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
    }

    default void createGroupMethod(MethodInfo methodInfo, ClassCreator invoker) {
        try (MethodCreator group = invoker.getMethodCreator("group", String.class)) {
            Optional.ofNullable(methodInfo
                    .annotation(DebeziumDotNames.CAPTURING)
                    .value("group"))
                    .map(AnnotationValue::asString)
                    .ifPresentOrElse(s -> {
                        if (s.isEmpty()) {
                            throw new IllegalArgumentException("empty groups are not allowed for @Capturing annotation  " + methodInfo.declaringClass());
                        }
                        group.returnValue(group.load(s));
                    },
                            () -> group.returnValue(group.load(Capturing.DEFAULT)));
        }
    }
}

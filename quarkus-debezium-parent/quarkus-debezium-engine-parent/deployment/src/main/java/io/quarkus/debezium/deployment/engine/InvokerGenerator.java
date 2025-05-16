/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.lang.reflect.Modifier;

import org.jboss.jandex.MethodInfo;

import io.debezium.engine.RecordChangeEvent;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.deployment.dotnames.DebeziumDotNames;
import io.quarkus.debezium.engine.CapturingInvoker;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.runtime.util.HashUtil;

public class InvokerGenerator {

    private final ClassOutput output;

    public InvokerGenerator(ClassOutput classOutput) {
        this.output = classOutput;
    }

    /**
     * it generates concreate classes based on the CapturingInvoker interface using gizmo:
     * <p>
     * public class GeneratedCapturingInvoker {
     *     private final Object beanInstance;
     *
     *     void capture(RecordChangeEvent<SourceRecord> event) {
     *         beanInstance.method(event);
     *     }
     *
     *     String getFullyQualifiedTableName() {
     *       return "tableQualifier";
     *     }
     * }
     *
     * @param methodInfo
     * @param beanInfo
     * @return
     */
    public InvokerMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        String name = generateClassName(beanInfo, methodInfo);

        try (ClassCreator invoker = ClassCreator.builder()
                .classOutput(this.output)
                .className(name)
                .interfaces(CapturingInvoker.class)
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

            try (MethodCreator capture = invoker.getMethodCreator("capture", void.class, RecordChangeEvent.class)) {
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
            Object qualifier = methodInfo
                    .annotation(DebeziumDotNames.CapturingDotName.CAPTURING)
                    .value().value();

            MethodCreator getTable = invoker.getMethodCreator("getFullyQualifiedTableName", String.class);
            getTable.returnValue(getTable.load(String.valueOf(qualifier)));

            return new InvokerMetaData(name.replace('/', '.'), beanInfo);
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

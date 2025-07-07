/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import java.lang.reflect.Modifier;
import java.util.UUID;

import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import io.debezium.relational.CustomConverterRegistry.ConverterDefinition;
import io.debezium.spi.converter.ConvertedField;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.debezium.engine.relational.converter.QuarkusCustomConverter;
import io.quarkus.gizmo.*;
import io.quarkus.runtime.util.HashUtil;

public class CustomConverterGenerator {
    private final ClassOutput output;

    public CustomConverterGenerator(ClassOutput output) {
        this.output = output;
    }

    /**
     * it generates concrete classes based on the QuarkusCustomConverter interface using gizmo:
     * <p>
     * public class GeneratedCustomConverter implements QuarkusCustomConverter {
     * private final Object converter;
     * private final Object filter;
     * <p>
     * public boolean filter(ConvertedField field) {
     * filter.filter(field);
     * }
     * <p>
     * <p>
     * ConverterDefinition<SchemaBuilder> bind(ConvertedField field) {
     * converter.method(key, value);
     * }
     * <p>
     * }
     *
     * @param methodInfo
     * @param converter
     * @param filter
     * @return
     */
    public GeneratedConverterClassMetaData generate(MethodInfo methodInfo, BeanInfo converter, BeanInfo filter) {
        String name = generateClassName(converter, methodInfo);

        try (ClassCreator quarkusCustomConverter = ClassCreator.builder()
                .classOutput(output)
                .className(name)
                .interfaces(QuarkusCustomConverter.class)
                .build()) {

            FieldDescriptor converterInstanceField = quarkusCustomConverter.getFieldCreator("converter", methodInfo
                    .declaringClass()
                    .name()
                    .toString())
                    .setModifiers(Modifier.PRIVATE)
                    .getFieldDescriptor();

            try (MethodCreator constructor = quarkusCustomConverter.getMethodCreator("<init>", void.class, Object.class)) {
                constructor.setModifiers(Modifier.PUBLIC);
                constructor.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructor.getThis());
                ResultHandle constructorThis = constructor.getThis();
                ResultHandle converterInstance = constructor.getMethodParam(0);
                constructor.writeInstanceField(converterInstanceField, constructorThis, converterInstance);
                constructor.returnValue(null);
            }

            try (MethodCreator convertMethod = quarkusCustomConverter.getMethodCreator("bind", ConverterDefinition.class, ConvertedField.class)) {
                ResultHandle convertThis = convertMethod.getThis();
                ResultHandle delegate = convertMethod.readInstanceField(converterInstanceField, convertThis);
                ResultHandle parameter = convertMethod.getMethodParam(0);

                MethodDescriptor methodDescriptor = MethodDescriptor.ofMethod(
                        methodInfo.declaringClass().toString(),
                        methodInfo.name(),
                        ConverterDefinition.class,
                        methodInfo.parameterType(0).name().toString());

                ResultHandle resultHandle = convertMethod.invokeVirtualMethod(methodDescriptor, delegate, parameter);

                convertMethod.returnValue(resultHandle);
            }

            if (filter != null) {
                FieldDescriptor filterInstanceField = quarkusCustomConverter.getFieldCreator("filter", filter
                        .getImplClazz()
                        .name()
                        .toString())
                        .setModifiers(Modifier.PRIVATE)
                        .getFieldDescriptor();

                try (MethodCreator constructor = quarkusCustomConverter.getMethodCreator("<init>", void.class, Object.class, Object.class)) {
                    constructor.setModifiers(Modifier.PUBLIC);

                    constructor.invokeSpecialMethod(MethodDescriptor.ofConstructor(Object.class), constructor.getThis());
                    ResultHandle constructorThis = constructor.getThis();
                    ResultHandle converterInstance = constructor.getMethodParam(0);
                    constructor.writeInstanceField(converterInstanceField, constructorThis, converterInstance);

                    ResultHandle filterInstance = constructor.getMethodParam(1);
                    constructor.writeInstanceField(filterInstanceField, constructorThis, filterInstance);

                    constructor.returnValue(null);
                }

                try (MethodCreator filterMethod = quarkusCustomConverter.getMethodCreator("filter", boolean.class, ConvertedField.class)) {
                    ResultHandle filterThis = filterMethod.getThis();
                    ResultHandle delegate = filterMethod.readInstanceField(filterInstanceField, filterThis);
                    ResultHandle parameter = filterMethod.getMethodParam(0);

                    MethodInfo delegateFilterMethod = filter.getImplClazz().method("filter", Type.create(ConvertedField.class));
                    MethodDescriptor methodDescriptor = MethodDescriptor.ofMethod(
                            filter.getImplClazz().toString(),
                            delegateFilterMethod.name(),
                            boolean.class,
                            delegateFilterMethod.parameterType(0).name().toString());

                    ResultHandle resultHandle = filterMethod.invokeVirtualMethod(methodDescriptor, delegate, parameter);

                    filterMethod.returnValue(resultHandle);
                }

            }
            return new GeneratedConverterClassMetaData(UUID.randomUUID(), name.replace('/', '.'), converter, filter);
        }
    }

    private String generateClassName(BeanInfo bean, MethodInfo methodInfo) {
        return DotNames.internalPackageNameWithTrailingSlash(bean.getImplClazz().name())
                + DotNames.simpleName(bean.getImplClazz().name())
                + "_DebeziumCustomConverter" + "_"
                + methodInfo.name() + "_"
                + HashUtil.sha1(methodInfo.name() + "_" + methodInfo.returnType().name().toString());
    }
}

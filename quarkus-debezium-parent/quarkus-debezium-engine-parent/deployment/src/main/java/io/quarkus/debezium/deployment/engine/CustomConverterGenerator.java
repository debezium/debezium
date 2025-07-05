/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import org.jboss.jandex.MethodInfo;

import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.deployment.GeneratedClassGizmoAdaptor;

public class CustomConverterGenerator {
    private final GeneratedClassGizmoAdaptor generatedClassGizmoAdaptor;

    public CustomConverterGenerator(GeneratedClassGizmoAdaptor generatedClassGizmoAdaptor) {
        this.generatedClassGizmoAdaptor = generatedClassGizmoAdaptor;
    }

    /**
     * it generates concrete classes based on the QuarkusCustomConverter interface using gizmo:
     * <p>
     * public class GeneratedCustomConverter implements QuarkusCustomConverter {
     *     private final Object beanInstance;
     *     private final Object filterInstance;
     *
     *     public boolean filter(ConvertedField convertedField) {
     *          filterInstance.filter(convertedField);
     *     }
     *
     *
     *     ConverterDefinition<SchemaBuilder> bind(ConvertedField field) {
     *         beanInstance.method(key, value);
     *     }
     *
     * }
     * @param methodInfo
     * @param beanInfo
     * @return
     */
    public GeneratedClassMetaData generate(MethodInfo methodInfo, BeanInfo beanInfo) {
        return null;
    }
}

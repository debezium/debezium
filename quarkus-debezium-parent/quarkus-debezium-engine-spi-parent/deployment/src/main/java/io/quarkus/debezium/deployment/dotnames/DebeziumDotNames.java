/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.dotnames;

import java.util.List;
import java.util.Objects;

import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CustomConverter;
import io.debezium.runtime.FieldFilterStrategy;
import io.debezium.runtime.PostProcessing;
import io.quarkus.arc.processor.BeanInfo;

public class DebeziumDotNames {

    public static final DotName DEBEZIUM_ENGINE_PROCESSOR = DotName.createSimple("io.quarkus.debezium.deployment.engine.EngineProcessor");
    public static final List<DotName> ANNOTATED_WITH_INJECT_SERVICE = List.of(
            DotName.createSimple("io.debezium.processors.PostProcessorRegistry"));
    public static final DotName CAPTURING = DotName.createSimple(Capturing.class.getName());
    public static final DotName POST_PROCESSING = DotName.createSimple(PostProcessing.class.getName());
    public static final DotName CUSTOM_CONVERTER = DotName.createSimple(CustomConverter.class.getName());
    public static final DotName FIELD_FILTER_STRATEGY = DotName.createSimple(FieldFilterStrategy.class.getName());
    public static final List<DotName> dotNames = List.of(CAPTURING, POST_PROCESSING, CUSTOM_CONVERTER);

    public boolean filter(BeanInfo info) {
        return info.getTarget()
                .map(annotation -> annotation.asClass().methods()
                        .stream()
                        .anyMatch(this::filter))
                .orElse(false);
    }

    public boolean filter(MethodInfo info) {
        return info.annotations()
                .stream()
                .anyMatch(instance -> dotNames
                        .stream()
                        .anyMatch(debeziumDotName -> debeziumDotName.equals(instance.name())));
    }

    public DotName get(MethodInfo info) {
        return dotNames
                .stream()
                .map(info::annotation)
                .filter(Objects::nonNull)
                .map(AnnotationInstance::name)
                .findFirst()
                .orElse(null);
    }

}

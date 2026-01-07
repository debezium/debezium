/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.connector.postgresql.TestHelper;
import io.debezium.junit.AnnotationBasedExtension;

/**
 * JUnit 5 extension that skips a test based on the {@link SkipWhenDecoderPluginNameIs} annotation or the
 * {@link SkipWhenDecoderPluginNameIsNot} annotation, on either a test method or a test class.
 */
public class SkipTestDependingOnDecoderPluginNameExtension extends AnnotationBasedExtension implements ExecutionCondition {
    private static final String pluginName = determineDecoderPluginName();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        SkipWhenDecoderPluginNameIs skipHasName = hasAnnotation(context, SkipWhenDecoderPluginNameIs.class);
        if (skipHasName != null && skipHasName.value().isEqualTo(pluginName)) {
            String reasonForSkipping = "Decoder plugin name is " + skipHasName.value() + System.lineSeparator() + skipHasName.reason();
            return ConditionEvaluationResult.disabled(reasonForSkipping);
        }

        SkipWhenDecoderPluginNameIsNot skipNameIsNot = hasAnnotation(context, SkipWhenDecoderPluginNameIsNot.class);
        if (skipNameIsNot != null && skipNameIsNot.value().isNotEqualTo(pluginName)) {
            String reasonForSkipping = "Decoder plugin name is not " + skipNameIsNot.value() + System.lineSeparator() + skipNameIsNot.reason();
            return ConditionEvaluationResult.disabled(reasonForSkipping);
        }

        return ConditionEvaluationResult.enabled("Decoder plugin name is compatible");
    }

    public static String determineDecoderPluginName() {
        return TestHelper.decoderPlugin().getPostgresPluginName();
    }
}

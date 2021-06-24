/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.connector.postgresql.TestHelper;
import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test based on the {@link SkipWhenDecoderPluginNameIs} annotation or the
 * {@link SkipWhenDecoderPluginNameIsNot} annotation, on either a test method or a test class.
 */
public class SkipTestDependingOnDecoderPluginNameRule extends AnnotationBasedTestRule {
    private static final String pluginName = determineDecoderPluginName();

    @Override
    public Statement apply(Statement base, Description description) {
        SkipWhenDecoderPluginNameIs skipHasName = hasAnnotation(description, SkipWhenDecoderPluginNameIs.class);
        if (skipHasName != null && skipHasName.value().isEqualTo(pluginName)) {
            String reasonForSkipping = "Decoder plugin name is " + skipHasName.value() + System.lineSeparator() + skipHasName.reason();
            return emptyStatement(reasonForSkipping, description);
        }

        SkipWhenDecoderPluginNameIsNot skipNameIsNot = hasAnnotation(description, SkipWhenDecoderPluginNameIsNot.class);
        if (skipNameIsNot != null && skipNameIsNot.value().isNotEqualTo(pluginName)) {
            String reasonForSkipping = "Decoder plugin name is not " + skipNameIsNot.value() + System.lineSeparator() + skipNameIsNot.reason();
            return emptyStatement(reasonForSkipping, description);
        }

        return base;
    }

    public static String determineDecoderPluginName() {
        return TestHelper.decoderPlugin().getPostgresPluginName();
    }
}

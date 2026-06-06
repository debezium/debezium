/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import java.util.Objects;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.data.VerifyRecord;
import io.debezium.junit.AnnotationBasedExtension;

/**
 * JUnit 5 extension that skips a test when a test is run against Apicurio registry.
 *
 * @author vjuranek
 */
public class SkipTestWhenRunWithApicurioExtension extends AnnotationBasedExtension implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        final SkipWhenRunWithApicurio option = hasAnnotation(context, SkipWhenRunWithApicurio.class);
        if (Objects.nonNull(option)) {
            if (VerifyRecord.isApucurioAvailable()) {
                return ConditionEvaluationResult.disabled("Test doesn't work with Apicurio registry");
            }
        }
        return ConditionEvaluationResult.enabled("Not running with Apicurio");
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.AnnotationBasedExtension;

/**
 * JUnit 5 extension that skips a test based on the {@link SkipOnReadOnly} annotation on
 * either a test method or test class.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnReadOnlyExtension extends AnnotationBasedExtension implements ExecutionCondition {

    private static final Configuration config = TestHelper.defaultConfig().build();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        SkipOnReadOnly skipOnReadOnly = hasAnnotation(context, SkipOnReadOnly.class);
        if (skipOnReadOnly != null) {
            if (config.getBoolean(OracleConnectorConfig.LOG_MINING_READ_ONLY)) {
                String reasonForSkipping = "Read Only: " + skipOnReadOnly.reason();
                return ConditionEvaluationResult.disabled(reasonForSkipping);
            }
        }
        return ConditionEvaluationResult.enabled("Not in read-only mode");
    }
}

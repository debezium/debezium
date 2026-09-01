/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.AnnotationBasedExtension;

/**
 * JUnit 5 extension that skips a test class based on the {@link SkipWhenNotAutonomous} annotation
 * when the test database is not an Oracle Autonomous Database.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnAutonomousExtension extends AnnotationBasedExtension implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        SkipWhenNotAutonomous skipWhenNotAutonomous = hasAnnotation(context, SkipWhenNotAutonomous.class);
        if (skipWhenNotAutonomous != null && !TestHelper.isAutonomousDatabase()) {
            String reasonForSkipping = "Database is not an Oracle Autonomous Database" + System.lineSeparator() + skipWhenNotAutonomous.reason();
            return ConditionEvaluationResult.disabled(reasonForSkipping);
        }

        return ConditionEvaluationResult.enabled("Database is an Oracle Autonomous Database");
    }
}
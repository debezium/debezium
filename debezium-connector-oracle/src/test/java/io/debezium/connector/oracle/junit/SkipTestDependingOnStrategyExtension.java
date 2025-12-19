/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.AnnotationBasedExtension;

/**
 * JUnit 5 extension that skips a test based on the {@link SkipWhenLogMiningStrategyIs} annotation on either a test
 * or a test class.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnStrategyExtension extends AnnotationBasedExtension implements ExecutionCondition {

    private static final String strategyName = determineStrategyName();

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        SkipWhenLogMiningStrategyIs strategyIs = hasAnnotation(context, SkipWhenLogMiningStrategyIs.class);
        if (strategyIs != null && strategyIs.value().isEqualTo(strategyName)) {
            String reason = "Strategy is " + strategyIs.value() + System.lineSeparator() + strategyIs.reason();
            return ConditionEvaluationResult.disabled(reason);
        }
        return ConditionEvaluationResult.enabled("Strategy is compatible");
    }

    public static String determineStrategyName() {
        final LogMiningStrategy strategy = TestHelper.logMiningStrategy();
        return strategy != null ? strategy.getValue() : null;
    }
}

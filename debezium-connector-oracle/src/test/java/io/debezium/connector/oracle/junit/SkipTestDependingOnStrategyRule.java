/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test based on the {@link SkipWhenLogMiningStrategyIs} annotation on either a test
 * or a test class.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnStrategyRule extends AnnotationBasedTestRule {

    private static final String strategyName = determineStrategyName();

    @Override
    public Statement apply(Statement base, Description description) {
        SkipWhenLogMiningStrategyIs strategyIs = hasAnnotation(description, SkipWhenLogMiningStrategyIs.class);
        if (strategyIs != null && strategyIs.value().isEqualTo(strategyName)) {
            String reason = "Strategy is " + strategyIs.value() + System.lineSeparator() + strategyIs.reason();
            return emptyStatement(reason, description);
        }
        return base;
    }

    public static String determineStrategyName() {
        final LogMiningStrategy strategy = TestHelper.logMiningStrategy();
        return strategy != null ? strategy.getValue() : null;
    }
}

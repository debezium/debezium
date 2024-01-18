/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test based on the {@link SkipOnReadOnly} annotation on
 * either a test method or test class.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnReadOnly extends AnnotationBasedTestRule {

    private static final Configuration config = TestHelper.defaultConfig().build();

    @Override
    public Statement apply(Statement base, Description description) {
        SkipOnReadOnly skipOnReadOnly = hasAnnotation(description, SkipOnReadOnly.class);
        if (skipOnReadOnly != null) {
            if (config.getBoolean(OracleConnectorConfig.LOG_MINING_READ_ONLY)) {
                String reasonForSkipping = "Read Only: " + skipOnReadOnly.reason();
                return emptyStatement(reasonForSkipping, description);
            }
        }
        return base;
    }
}

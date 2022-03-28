/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test based on the {@link SkipWhenSslModeIsNot} annotation on either a test method or a test class.
 */
public class SkipTestDependingOnSslModeRule extends AnnotationBasedTestRule {
    private static final String CURRENT_SSL_MODE = System.getProperty("database.ssl.mode", "disabled");

    @Override
    public Statement apply(Statement base, Description description) {
        SkipWhenSslModeIsNot skipSslMode = hasAnnotation(description, SkipWhenSslModeIsNot.class);
        if (skipSslMode != null && !skipSslMode.value().name().equalsIgnoreCase(CURRENT_SSL_MODE)) {
            String reasonForSkipping = "Current SSL_MODE is " + CURRENT_SSL_MODE + System.lineSeparator() + skipSslMode.reason();
            return emptyStatement(reasonForSkipping, description);
        }
        return base;
    }
}

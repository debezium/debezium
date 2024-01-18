/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import java.util.Objects;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.data.VerifyRecord;
import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test when a test is run against Apicurio registry.
 *
 * @author vjuranek
 */
public class SkipTestWhenRunWithApicurioRule extends AnnotationBasedTestRule {

    @Override
    public Statement apply(Statement base, Description description) {
        final SkipWhenRunWithApicurio option = hasAnnotation(description, SkipWhenRunWithApicurio.class);
        if (Objects.nonNull(option)) {
            if (VerifyRecord.isApucurioAvailable()) {
                return emptyStatement("Test doesn't work with Apicurio registry", description);
            }
        }
        return base;
    }
}

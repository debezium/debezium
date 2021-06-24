/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.AnnotationBasedTestRule;

/**
 * JUnit rule that skips a test based on the {@link SkipWhenAdapterNameIs} annotation on
 * either a test method or test class.
 *
 * @author Chris Cranford
 */
public class SkipTestDependingOnAdapterNameRule extends AnnotationBasedTestRule {

    private static final String adapterName = determineAdapterName();

    @Override
    public Statement apply(Statement base, Description description) {
        SkipWhenAdapterNameIs skipWhenAdpterName = hasAnnotation(description, SkipWhenAdapterNameIs.class);
        if (skipWhenAdpterName != null && skipWhenAdpterName.value().isEqualTo(adapterName)) {
            String reasonForSkipping = "Adapter name is " + skipWhenAdpterName.value() + System.lineSeparator() + skipWhenAdpterName.reason();
            return emptyStatement(reasonForSkipping, description);
        }

        SkipWhenAdapterNameIsNot skipWhenAdapterNameNot = hasAnnotation(description, SkipWhenAdapterNameIsNot.class);
        if (skipWhenAdapterNameNot != null && skipWhenAdapterNameNot.value().isNotEqualTo(adapterName)) {
            String reasonForSkipping = "Adapter name is not " + skipWhenAdapterNameNot.value() + System.lineSeparator() + skipWhenAdapterNameNot.reason();
            return emptyStatement(reasonForSkipping, description);
        }

        return base;
    }

    public static String determineAdapterName() {
        return TestHelper.adapter().getValue();
    }
}

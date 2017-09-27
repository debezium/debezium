/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule that inspects the presence of the {@link SkipLongRunning} annotation either on a test method or on a test suite. If
 * it finds the annotation, it will only run the test method/suite if the system property {@code skipLongRunningTests} has the
 * value {@code true}
 * 
 * @author Horia Chiorean
 */
public class SkipTestRule extends AnnotationBasedTestRule {

    @Override
    public Statement apply( Statement base,
                            Description description ) {
        SkipLongRunning skipLongRunningAnnotation = hasAnnotation(description, SkipLongRunning.class);
        if (skipLongRunningAnnotation != null) {
            String skipLongRunning = System.getProperty(SkipLongRunning.SKIP_LONG_RUNNING_PROPERTY);
            if (skipLongRunning == null || Boolean.valueOf(skipLongRunning)) {
                return emptyStatement(skipLongRunningAnnotation.value(), description);
            }
        }

        SkipOnOS skipOnOSAnnotation = hasAnnotation(description, SkipOnOS.class);
        if (skipOnOSAnnotation != null) {
            String[] oses = skipOnOSAnnotation.value();
            String osName = System.getProperty("os.name");
            if (osName != null && !osName.trim().isEmpty()) {
                for (String os : oses) {
                    if (osName.toLowerCase().startsWith(os.toLowerCase())) {
                        return emptyStatement(skipOnOSAnnotation.description(), description);
                    }
                }
            }
        }

        return base;
    }
}

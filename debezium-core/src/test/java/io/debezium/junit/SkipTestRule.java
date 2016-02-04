/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.Annotation;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * JUnit rule that inspects the presence of the {@link SkipLongRunning} annotation either on a test method or on a test suite. If
 * it finds the annotation, it will only run the test method/suite if the system property {@code skipLongRunningTests} has the
 * value {@code true}
 * 
 * @author Horia Chiorean
 */
public class SkipTestRule implements TestRule {

    @Override
    public Statement apply( Statement base,
                            Description description ) {
        SkipLongRunning skipLongRunningAnnotation = hasAnnotation(description, SkipLongRunning.class);
        if (skipLongRunningAnnotation != null) {
            boolean skipLongRunning = Boolean.valueOf(System.getProperty(SkipLongRunning.SKIP_LONG_RUNNING_PROPERTY));
            if (skipLongRunning) {
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

    private <T extends Annotation> T hasAnnotation( Description description, Class<T> annotationClass ) {
        T annotation = description.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        } else if (description.isTest() && description.getTestClass().isAnnotationPresent(annotationClass)) {
            return description.getTestClass().getAnnotation(annotationClass);
        }
        return null;
    }

    private static Statement emptyStatement( final String reason, final Description description ) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                StringBuilder messageBuilder = new StringBuilder(description.testCount());
                messageBuilder.append("Skipped ").append(description.toString());
                if (reason != null && !reason.trim().isEmpty()) {
                    messageBuilder.append(" because: ").append(reason);
                }
                System.out.println(messageBuilder.toString());
            }
        };
    }
}
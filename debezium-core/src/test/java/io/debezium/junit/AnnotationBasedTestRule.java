/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A base {@link TestRule} that allows easy writing of test rules based on method annotations.
 *
 * @author Jiri Pechanec
 */
public abstract class AnnotationBasedTestRule implements TestRule {

    protected static Statement emptyStatement(final String reason, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                StringBuilder messageBuilder = new StringBuilder(description.testCount());
                messageBuilder.append("Skipped ").append(description);
                if (reason != null && !reason.trim().isEmpty()) {
                    messageBuilder.append(" because: ").append(reason);
                }

                System.out.println(messageBuilder);
            }
        };
    }

    protected <T extends Annotation> T hasAnnotation(Description description, Class<T> annotationClass) {
        T annotation = description.getAnnotation(annotationClass);

        if (annotation != null) {
            return annotation;
        }
        else if (description.isTest() && description.getTestClass().isAnnotationPresent(annotationClass)) {
            return description.getTestClass().getAnnotation(annotationClass);
        }
        else if (description.isTest()) {
            // Check for the annotation on the class that declares the test method
            for (Method method : description.getTestClass().getMethods()) {
                if (method.getName().equals(description.getMethodName())) {
                    if (method.getDeclaringClass().isAnnotationPresent(annotationClass)) {
                        return method.getDeclaringClass().getAnnotation(annotationClass);
                    }
                }
            }
        }

        return null;
    }
}

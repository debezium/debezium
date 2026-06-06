/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Optional;

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A base class for JUnit 5 Extensions that allows easy writing of extensions based on method annotations.
 * This is the JUnit 5 equivalent of {@link AnnotationBasedTestRule}.
 *
 * @author Jiri Pechanec
 */
public abstract class AnnotationBasedExtension {

    /**
     * Check if the test method or test class has the specified annotation.
     *
     * @param context the extension context
     * @param annotationClass the annotation class to check for
     * @param <T> the annotation type
     * @return the annotation if present, null otherwise
     */
    protected <T extends Annotation> T hasAnnotation(ExtensionContext context, Class<T> annotationClass) {
        // First check the test method
        Optional<Method> testMethod = context.getTestMethod();
        if (testMethod.isPresent()) {
            T annotation = testMethod.get().getAnnotation(annotationClass);
            if (annotation != null) {
                return annotation;
            }
        }

        // Then check the test class
        Optional<Class<?>> testClass = context.getTestClass();
        if (testClass.isPresent()) {
            T annotation = testClass.get().getAnnotation(annotationClass);
            if (annotation != null) {
                return annotation;
            }

            // Also check the declaring class of the test method (for inherited test methods)
            if (testMethod.isPresent()) {
                Class<?> declaringClass = testMethod.get().getDeclaringClass();
                if (declaringClass.isAnnotationPresent(annotationClass)) {
                    return declaringClass.getAnnotation(annotationClass);
                }
            }
        }

        return null;
    }

    /**
     * Format a skip message for logging.
     *
     * @param reason the reason for skipping
     * @param context the extension context
     * @return formatted message
     */
    protected String formatSkipMessage(String reason, ExtensionContext context) {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append("Skipped ");

        context.getTestClass().ifPresent(testClass -> {
            messageBuilder.append(testClass.getName());
            context.getTestMethod().ifPresent(method -> {
                messageBuilder.append("#").append(method.getName());
            });
        });

        if (reason != null && !reason.trim().isEmpty()) {
            messageBuilder.append(" because: ").append(reason);
        }

        return messageBuilder.toString();
    }
}

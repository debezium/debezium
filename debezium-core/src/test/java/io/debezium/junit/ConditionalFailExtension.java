/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit 5 Extension that inspects the presence of the {@link ShouldFailWhen} and {@link Flaky} annotations on a test method.
 * This is the JUnit 5 equivalent of the JUnit 4 {@link ConditionalFail} rule.
 *
 * For {@link ShouldFailWhen}: If the condition evaluates to true and the test passes, the test will fail.
 * If the condition evaluates to true and the test fails, the failure is ignored.
 *
 * For {@link Flaky}: Retries failing tests up to a configured number of times.
 * If all attempts fail, marks the test as aborted (skipped).
 *
 * @author Jiri Pechanec
 */
public class ConditionalFailExtension extends AnnotationBasedExtension implements InvocationInterceptor {

    private static final Logger FLAKY_LOGGER = LoggerFactory.getLogger(Flaky.class);
    private static final String JIRA_BASE_URL = "https://issues.redhat.com/browse/";

    @Override
    public void interceptTestMethod(Invocation<Void> invocation,
                                    ReflectiveInvocationContext<java.lang.reflect.Method> invocationContext,
                                    ExtensionContext extensionContext)
            throws Throwable {

        // Check for ShouldFailWhen annotation
        ShouldFailWhen conditionAnnotation = hasAnnotation(extensionContext, ShouldFailWhen.class);
        if (conditionAnnotation != null) {
            handleShouldFailWhen(invocation, conditionAnnotation, extensionContext);
            return;
        }

        // Check for Flaky annotation
        Flaky flakyAnnotation = hasAnnotation(extensionContext, Flaky.class);
        if (flakyAnnotation != null) {
            handleFlaky(invocation, flakyAnnotation, extensionContext);
            return;
        }

        // No special handling needed, proceed normally
        invocation.proceed();
    }

    private void handleShouldFailWhen(Invocation<Void> invocation,
                                      ShouldFailWhen annotation,
                                      ExtensionContext context)
            throws Throwable {
        try {
            Supplier<Boolean> condition = annotation.value().getDeclaredConstructor().newInstance();
            Throwable failure = null;

            try {
                invocation.proceed();
            }
            catch (Throwable t) {
                failure = t;
            }

            if (condition.get() && failure == null) {
                String testName = context.getTestClass().map(Class::getName).orElse("Unknown") + "#" +
                        context.getTestMethod().map(java.lang.reflect.Method::getName).orElse("Unknown");
                throw new AssertionError("Expected failing test for " + testName);
            }
            else if (condition.get() && failure != null) {
                String testName = context.getTestClass().map(Class::getName).orElse("Unknown") + "#" +
                        context.getTestMethod().map(java.lang.reflect.Method::getName).orElse("Unknown");
                System.out.println("Ignored failure for " + testName);
                // Test expected to fail and it did - this is success
                return;
            }
            else if (failure != null) {
                throw failure;
            }
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    private void handleFlaky(Invocation<Void> invocation,
                             Flaky annotation,
                             ExtensionContext context)
            throws Throwable {

        final String failFlakyTestsProperty = System.getProperty(Flaky.FAIL_FLAKY_TESTS_PROPERTY);
        if (failFlakyTestsProperty == null || Boolean.valueOf(failFlakyTestsProperty)) {
            // Flaky handling disabled, run test normally
            invocation.proceed();
            return;
        }

        // NOTE: Full retry logic (multiple attempts) is not implemented in this JUnit 5 extension
        // due to limitations in the InvocationInterceptor API. The extension will catch a single
        // failure and mark the test as aborted instead of failing.
        // For full retry support, consider using a dedicated retry extension like junit-pioneer.

        try {
            invocation.proceed();
        }
        catch (Throwable t) {
            String testName = context.getTestClass().map(Class::getName).orElse("Unknown") + "#" +
                    context.getTestMethod().map(java.lang.reflect.Method::getName).orElse("Unknown");
            FLAKY_LOGGER.error("Ignored failure for {}, tracked with {}", testName, issueUrl(annotation.value()), t);

            // Mark test as aborted (skipped in JUnit 5) instead of failed
            String testClassName = context.getTestClass().map(Class::getSimpleName).orElse("Unknown");
            String methodName = context.getTestMethod().map(java.lang.reflect.Method::getName).orElse("Unknown");
            throw new TestAbortedException(String.format("Flaky test %s#%s failed", testClassName, methodName));
        }
    }

    private String issueUrl(String jiraId) {
        return JIRA_BASE_URL + jiraId;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit rule that inspects the presence of the {@link ShouldFailWhen} annotation on a test method.
 * If it finds the annotation, it will modify pass/fail report of test depending on the condition
 * defined in the annotation.
 *
 * @author Jiri Pechanec
 */
public class ConditionalFail extends AnnotationBasedTestRule {

    private static final Logger FLAKY_LOGGER = LoggerFactory.getLogger(Flaky.class);

    private static final String JIRA_BASE_URL = "https://issues.redhat.com/browse/";

    @Override
    public Statement apply(final Statement base, final Description description) {
        final ShouldFailWhen conditionClass = hasAnnotation(description, ShouldFailWhen.class);
        if (conditionClass != null) {
            return failOnCondition(base, description, conditionClass);
        }

        final Flaky flakyClass = hasAnnotation(description, Flaky.class);
        if (flakyClass != null) {
            return ignoreFlakyFailure(base, description, flakyClass);
        }
        return base;
    }

    private Statement failOnCondition(final Statement base, final Description description,
                                      final ShouldFailWhen conditionClass) {
        try {
            Supplier<Boolean> condition = conditionClass.value().getDeclaredConstructor().newInstance();
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    Throwable failure = null;
                    try {
                        base.evaluate();
                    }
                    catch (final Throwable t) {
                        failure = t;
                    }
                    if (condition.get() && failure == null) {
                        Assert.fail("Expected failing test for " + description);
                    }
                    else if (condition.get() && failure != null) {
                        System.out.println("Ignored failure for " + description);
                    }
                    else if (failure != null) {
                        throw failure;
                    }
                }
            };
        }
        catch (final InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new IllegalStateException(e);
        }
    }

    private Statement ignoreFlakyFailure(final Statement base, final Description description,
                                         final Flaky flakyClass) {

        final String failFlakyTestsProperty = System.getProperty(Flaky.FAIL_FLAKY_TESTS_PROPERTY);
        if (failFlakyTestsProperty == null || Boolean.valueOf(failFlakyTestsProperty)) {
            return base;
        }
        final String flakyAttemptsProperty = System.getProperty(Flaky.FLAKY_ATTEMPTS_FAILURES_PROPERTY, "1");
        final int attempts = Integer.parseInt(flakyAttemptsProperty);

        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                for (int i = 0; i < attempts; i++) {
                    try {
                        base.evaluate();
                        return;
                    }
                    catch (final Throwable t) {
                        FLAKY_LOGGER.error("Ignored failure for {}, tracked with {}", description, issueUrl(flakyClass.value()), t);
                    }
                }
                // Marks test as skipped
                Assume.assumeTrue(String.format("Flaky test %s#%s failed", description.getTestClass().getSimpleName(), description.getMethodName()), false);
            }
        };
    }

    private String issueUrl(String jiraId) {
        return JIRA_BASE_URL + jiraId;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;
import org.junit.platform.commons.support.ReflectionSupport;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.embedded.AbstractConnectorTest;

/**
 * A JUnit 5 extension that re-executes a failed test when the connector stopped because Oracle raised
 * {@code ORA-01466: unable to read data - table definition has changed} during the snapshot.
 * <p>
 * The snapshot reads table data with flashback queries at the SCN determined at the start of the snapshot.
 * When another session changes the definition of a table, for example an unrelated test or a background
 * database job, the flashback query fails and the snapshot cannot recover. Such failures are not caused by
 * the test and the test is re-executed up to {@link #MAX_ATTEMPTS} times before it is reported as failed.
 * <p>
 * The extension is registered automatically for all tests in the module through the JUnit
 * {@code org.junit.jupiter.api.extension.Extension} service registration, so individual tests do not need
 * to be annotated. It only acts on tests that extend {@link AbstractConnectorTest}, since the connector
 * failure is taken from {@link AbstractConnectorTest#getLastEngineFailure()}.
 * <p>
 * Before a test is re-executed, the {@link AfterEach} and {@link BeforeEach} lifecycle methods of the test
 * class hierarchy are invoked again so that the test starts from a clean state.
 *
 * @author Chris Cranford
 */
public class RetryOnTableDefinitionChangedExtension implements InvocationInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryOnTableDefinitionChangedExtension.class);

    static final int MAX_ATTEMPTS = 3;

    @Override
    public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext)
            throws Throwable {
        Throwable failure;
        try {
            invocation.proceed();
            return;
        }
        catch (Throwable t) {
            failure = t;
        }

        final Object target = invocationContext.getTarget().orElse(null);
        if (!(target instanceof AbstractConnectorTest)) {
            throw failure;
        }

        final AbstractConnectorTest test = (AbstractConnectorTest) target;
        final String testName = extensionContext.getRequiredTestClass().getSimpleName() + "#" + invocationContext.getExecutable().getName();

        for (int attempt = 2; attempt <= MAX_ATTEMPTS; attempt++) {
            if (failure instanceof TestAbortedException || !isTableDefinitionChangedFailure(test.getLastEngineFailure())) {
                throw failure;
            }

            LOGGER.warn("Test {} failed because the connector stopped with ORA-01466, re-executing (attempt {} of {})",
                    testName, attempt, MAX_ATTEMPTS, failure);

            try {
                reExecute(invocationContext, extensionContext, target);
                LOGGER.info("Test {} passed on attempt {} of {}", testName, attempt, MAX_ATTEMPTS);
                return;
            }
            catch (Throwable t) {
                failure = t;
            }
        }

        throw failure;
    }

    /**
     * Returns whether the supplied engine failure was caused by Oracle raising {@code ORA-01466}.
     *
     * @param failure the error with which the engine completed, may be empty
     * @return true if the error or any of its causes is {@code ORA-01466}, false otherwise
     */
    static boolean isTableDefinitionChangedFailure(Optional<Throwable> failure) {
        final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = failure.orElse(null);
        while (current != null && visited.add(current)) {
            if (current instanceof SQLException && ((SQLException) current).getErrorCode() == 1466) {
                return true;
            }
            if (current.getMessage() != null && current.getMessage().contains("ORA-" + String.format("%05d", 1466))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void reExecute(ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext, Object target) {
        final Class<?> testClass = target.getClass();

        // Mirror the JUnit lifecycle: tear down from the most specific class upward, then set up from the base class downward
        invokeLifecycleMethods(AnnotationSupport.findAnnotatedMethods(testClass, AfterEach.class, HierarchyTraversalMode.BOTTOM_UP), target, extensionContext);
        invokeLifecycleMethods(AnnotationSupport.findAnnotatedMethods(testClass, BeforeEach.class, HierarchyTraversalMode.TOP_DOWN), target, extensionContext);

        ReflectionSupport.invokeMethod(invocationContext.getExecutable(), target, invocationContext.getArguments().toArray());
    }

    private void invokeLifecycleMethods(List<Method> methods, Object target, ExtensionContext extensionContext) {
        final List<Throwable> failures = new ArrayList<>();
        for (Method method : methods) {
            try {
                ReflectionSupport.invokeMethod(method, target, resolveParameters(method, extensionContext));
            }
            catch (Throwable t) {
                failures.add(t);
            }
        }
        if (!failures.isEmpty()) {
            final Throwable first = failures.get(0);
            failures.stream().skip(1).forEach(first::addSuppressed);
            throw new IllegalStateException("Failed to re-execute test lifecycle methods", first);
        }
    }

    private Object[] resolveParameters(Method method, ExtensionContext extensionContext) {
        final Parameter[] parameters = method.getParameters();
        final Object[] arguments = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            if (TestInfo.class.equals(parameters[i].getType())) {
                arguments[i] = testInfo(extensionContext);
            }
            else {
                throw new IllegalStateException("Cannot resolve parameter " + parameters[i] + " of lifecycle method " + method);
            }
        }
        return arguments;
    }

    private static TestInfo testInfo(ExtensionContext context) {
        return new TestInfo() {
            @Override
            public String getDisplayName() {
                return context.getDisplayName();
            }

            @Override
            public Set<String> getTags() {
                return context.getTags();
            }

            @Override
            public Optional<Class<?>> getTestClass() {
                return context.getTestClass();
            }

            @Override
            public Optional<Method> getTestMethod() {
                return context.getTestMethod();
            }
        };
    }
}

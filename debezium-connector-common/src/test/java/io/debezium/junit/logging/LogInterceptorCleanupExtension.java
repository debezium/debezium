/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstanceFactoryContext;
import org.junit.jupiter.api.extension.TestInstancePreConstructCallback;

/**
 * JUnit 5 extension that, after each test method, restores the logger levels changed through
 * {@link LogInterceptor#setLoggerLevel} or {@link LogInterceptor#changeLoggerLevel} and detaches every
 * {@link LogInterceptor} created for that test, including those held in instance fields.
 * <p>
 * Interceptors created when the test class is initialized, such as those held in static fields, or in
 * {@code @BeforeAll} methods are shared by the test methods, so they are only cleaned up after all tests
 * of the class have run.
 * <p>
 * It is registered globally through extension autodetection, so every test that uses the
 * {@code debezium-connector-common} test-jar is covered.
 *
 * @author Chris Cranford
 */
public class LogInterceptorCleanupExtension
        implements BeforeAllCallback, TestInstancePreConstructCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

    private static final String MARK = "mark";

    // The marker taken just before a test instance is constructed, so that interceptors held in instance
    // fields belong to the test that the instance is created for (or to the class with a per-class lifecycle).
    private static Long pendingMark;

    @Override
    public void preConstructTestInstance(TestInstanceFactoryContext factoryContext, ExtensionContext context) {
        if (pendingMark == null) {
            pendingMark = LogInterceptorRegistry.mark();
            LogInterceptorRegistry.enterScope(pendingMark);
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        store(context).put(MARK, takeMark());
        // Interceptors in static fields are registered as class scoped even when the class is only
        // initialized as the first test instance is constructed.
        LogInterceptorRegistry.enterClassScope(context.getRequiredTestClass());
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        store(context).put(MARK, takeMark());
    }

    @Override
    public void afterEach(ExtensionContext context) {
        LogInterceptorRegistry.cleanup(markOf(context));
    }

    @Override
    public void afterAll(ExtensionContext context) {
        LogInterceptorRegistry.exitClassScope(context.getRequiredTestClass());
        LogInterceptorRegistry.cleanup(markOf(context));
    }

    private static long takeMark() {
        final long mark = pendingMark != null ? pendingMark : LogInterceptorRegistry.mark();
        pendingMark = null;
        LogInterceptorRegistry.enterScope(mark);
        return mark;
    }

    private static long markOf(ExtensionContext context) {
        if (context == null) {
            return 0L;
        }
        final Long mark = store(context).get(MARK, Long.class);
        return mark != null ? mark : 0L;
    }

    private static ExtensionContext.Store store(ExtensionContext context) {
        // Namespaced by the unique id, so a test method never reads the marker of its class through the parent store.
        return context.getStore(ExtensionContext.Namespace.create(LogInterceptorCleanupExtension.class, context.getUniqueId()));
    }
}

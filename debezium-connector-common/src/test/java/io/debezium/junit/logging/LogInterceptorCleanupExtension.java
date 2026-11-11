/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that, after each test method, restores the logger levels changed through
 * {@link LogInterceptor#setLoggerLevel} or {@link LogInterceptor#changeLoggerLevel} and detaches every
 * {@link LogInterceptor} created during the test.
 * <p>
 * It is registered globally through extension autodetection, so every test that uses the
 * {@code debezium-connector-common} test-jar is covered.
 *
 * @author Chris Cranford
 */
public class LogInterceptorCleanupExtension implements AfterEachCallback {

    @Override
    public void afterEach(ExtensionContext context) {
        LogInterceptorRegistry.cleanup();
    }
}

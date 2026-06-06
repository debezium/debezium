/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.util.Optional;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit 5 Extension that logs start and end of each test method execution.
 *
 * @author Jiri Pechanec
 */
public class TestLoggerExtension implements TestWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestLoggerExtension.class);

    private final Logger logger;

    public TestLoggerExtension() {
        this.logger = LOGGER;
    }

    public TestLoggerExtension(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        String className = context.getTestClass().map(Class::getName).orElse("Unknown");
        String methodName = context.getTestMethod().map(java.lang.reflect.Method::getName).orElse("Unknown");
        logger.info("Test {}#{} disabled{}", className, methodName,
                reason.map(r -> " - " + r).orElse(""));
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        String className = context.getTestClass().map(Class::getName).orElse("Unknown");
        String methodName = context.getTestMethod().map(java.lang.reflect.Method::getName).orElse("Unknown");
        logger.info("Test {}#{} succeeded", className, methodName);
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        String className = context.getTestClass().map(Class::getName).orElse("Unknown");
        String methodName = context.getTestMethod().map(java.lang.reflect.Method::getName).orElse("Unknown");
        logger.info("Test {}#{} aborted", className, methodName);
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        String className = context.getTestClass().map(Class::getName).orElse("Unknown");
        String methodName = context.getTestMethod().map(java.lang.reflect.Method::getName).orElse("Unknown");
        logger.info("Test {}#{} failed", className, methodName);
    }
}

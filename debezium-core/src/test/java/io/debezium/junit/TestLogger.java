/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;

/**
 * JUnit rule that logs start and end of each test method execution.
 *
 * @author Jiri Pechanec
 *
 */
public class TestLogger extends TestWatcher {

    private final Logger logger;

    public TestLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void starting(Description description) {
        logger.info("Starting test {}#{}", description.getClassName(), description.getMethodName());
    }

    @Override
    public void succeeded(Description description) {
        logger.info("Test {}#{} succeeded", description.getClassName(), description.getMethodName());
    }

    @Override
    protected void failed(Throwable e, Description description) {
        logger.info("Test {}#{} failed", description.getClassName(), description.getMethodName());
    }
}

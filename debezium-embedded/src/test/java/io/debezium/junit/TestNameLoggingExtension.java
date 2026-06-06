/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs the name of the test class and test display name before each test execution.
 *
 * <p>While running tests on CI/CD platforms, particularly where the failsafe and surefire output
 * is not available, we need to rely on the logs to identify problems. It is not easy to identify
 * such problems when you have consecutive tests fail with similar problems.
 *
 * <p>This extension is automatically registered by JUnit's ServiceLoader by using the registration
 * in {@code org.junit.jupiter.api.extension.Extension}. This allows for the auto-registration of
 * all extensions without needing to apply {@link org.junit.jupiter.api.extension.ExtendWith}
 * annotations to classes.
 *
 * @author Chris Cranford
 */
public class TestNameLoggingExtension implements BeforeEachCallback {
    private final Logger LOGGER = LoggerFactory.getLogger(TestNameLoggingExtension.class);

    @Override
    public void beforeEach(ExtensionContext context) {
        LOGGER.info("Running test {}: {}", context.getRequiredTestClass().getName(), context.getDisplayName());
    }
}

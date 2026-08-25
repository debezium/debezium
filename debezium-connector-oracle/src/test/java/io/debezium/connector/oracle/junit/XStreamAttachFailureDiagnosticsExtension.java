/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.junit.logging.LogInterceptor;

/**
 * JUnit 5 extension that dumps the state of the XStream outbound server whenever a test failed to
 * attach to it.
 *
 * <p>Attach failures, in practice {@code ORA-26812}, have been observed to wedge the outbound server
 * for the remainder of a test run: the connector detaches cleanly, and every later attach is refused.
 * The connector logs cannot distinguish a genuinely orphaned client session from an outbound server
 * that only believes one is attached, so this extension captures the database side state at the point
 * of failure. It is diagnostic only, reads nothing but dictionary and dynamic performance views, and
 * never affects the outcome of a test.
 *
 * <p>Registered automatically for the module via {@code META-INF/services}, since extension
 * autodetection is enabled for the build.
 *
 * @author Chris Cranford
 */
public class XStreamAttachFailureDiagnosticsExtension implements BeforeEachCallback, AfterEachCallback {

    /**
     * Referenced by name rather than by class: the {@code xstream} package is excluded from the build
     * unless the {@code oracle-xstream} profile is active, so a direct class reference would not compile
     * for the other adapters.
     */
    private static final String XSTREAM_SOURCE_LOGGER = "io.debezium.connector.oracle.xstream.XstreamStreamingChangeEventSource";

    /**
     * Logged by the XStream streaming source for every failed attach attempt, and again when the
     * retries are exhausted.
     */
    private static final String ATTACH_FAILURE_MESSAGE = "Failed to attach to outbound server";

    /**
     * A {@link LogInterceptor} registers itself as a logback appender and offers no way to detach, so a
     * single instance is reused and cleared between tests rather than created per test.
     */
    private static LogInterceptor interceptor;

    @Override
    public void beforeEach(ExtensionContext context) {
        if (!TestHelper.isXStream()) {
            return;
        }
        if (interceptor == null) {
            interceptor = new LogInterceptor(XSTREAM_SOURCE_LOGGER);
        }
        interceptor.clear();
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (!TestHelper.isXStream() || interceptor == null) {
            return;
        }
        if (interceptor.containsMessage(ATTACH_FAILURE_MESSAGE)) {
            TestHelper.logXStreamOutboundServerDiagnostics("after " + context.getDisplayName());
        }
        interceptor.clear();
    }
}
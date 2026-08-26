/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.junit;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.util.TestHelper;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

/**
 * JUnit 5 extension that captures the state of the XStream outbound server whenever a test fails to
 * attach to it.
 *
 * <p>Attach failures, in practice {@code ORA-26812}, have been observed to wedge the outbound server
 * for the remainder of a test run: the connector detaches cleanly, and every later attach is refused.
 * The connector logs cannot distinguish a genuinely orphaned client session from an outbound server
 * that only believes one is attached, so this extension captures the database side state instead. It
 * is diagnostic only, reads nothing but dictionary and dynamic performance views, and never affects
 * the outcome of a test.
 *
 * <p>The full dump is triggered from a log appender rather than from {@link #afterEach}, because a
 * test that cannot attach then sits in its consume loop against the XStream poll timeout: measured
 * over a wedged run, {@code afterEach} ran a median of six minutes, and up to sixteen, behind the
 * failure it was meant to describe. Firing from the appender captures the server within a fraction of
 * a second of the failed attach, while {@code afterEach} remains as a fallback should that dump fail.
 *
 * <p>Separately, and on every test rather than only failing ones, the identity of the outbound
 * server's capture and apply sessions is tracked so that a restart on an ordinary detach can be told
 * apart from one peculiar to the wedge. See
 * {@link TestHelper#getXStreamOutboundServerSessionIdentity()}.
 *
 * <p>Registered automatically for the module via {@code META-INF/services}, since extension
 * autodetection is enabled for the build.
 *
 * @author Chris Cranford
 */
public class XStreamAttachFailureDiagnosticsExtension implements BeforeEachCallback, AfterEachCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(XStreamAttachFailureDiagnosticsExtension.class);

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
     * Attached to the streaming source's logger once and reused, since logback offers no way to detach
     * an appender and a per-test instance would accumulate over a run.
     */
    private static AttachFailureAppender appender;

    /**
     * Last observed identity of the outbound server's capture and apply sessions, so that only changes
     * are reported. Static because the comparison spans test classes.
     */
    private static String lastSessionIdentity;

    @Override
    public void beforeEach(ExtensionContext context) {
        if (!TestHelper.isXStream()) {
            return;
        }
        if (appender == null) {
            appender = new AttachFailureAppender();
            final ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) LoggerFactory
                    .getLogger(XSTREAM_SOURCE_LOGGER);
            appender.setContext(logger.getLoggerContext());
            appender.start();
            logger.addAppender(appender);
        }
        appender.resetFor(context.getDisplayName());
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (!TestHelper.isXStream() || appender == null) {
            return;
        }

        // Fallback only. Under normal operation the appender has already dumped, far closer to the
        // failure than this point; this covers the dump itself having failed.
        if (appender.sawFailureWithoutDump()) {
            TestHelper.logXStreamOutboundServerDiagnostics("after " + context.getDisplayName()
                    + " (fallback, inline capture did not complete)");
        }

        // Tracked on every boundary, not just failing ones: the point is to establish whether the
        // outbound server's capture and apply sessions restart on an ordinary detach. Only changes are
        // logged, so a run in which they restart on every test looks obviously different from one in
        // which they restart once. A lookup failure changes the identity too, so it cannot pass silently.
        final String identity = TestHelper.getXStreamOutboundServerSessionIdentity();
        if (identity != null && !identity.equals(lastSessionIdentity)) {
            LOGGER.warn("XStream outbound server sessions changed after {}{}  before: {}{}  after:  {}",
                    context.getDisplayName(), System.lineSeparator(),
                    lastSessionIdentity == null ? "<first observation>" : lastSessionIdentity,
                    System.lineSeparator(), identity);
            lastSessionIdentity = identity;
        }
    }

    /**
     * Fires the diagnostics collection on the first failed attach of each test, from the thread that
     * logged it, so the outbound server is observed while the connector is still retrying.
     */
    private static final class AttachFailureAppender extends AppenderBase<ILoggingEvent> {

        private final AtomicBoolean sawFailure = new AtomicBoolean();
        private final AtomicBoolean dumped = new AtomicBoolean();
        private volatile String testName = "<unknown test>";

        void resetFor(String testName) {
            this.testName = testName;
            sawFailure.set(false);
            dumped.set(false);
        }

        /**
         * @return whether this test saw an attach failure that the inline capture did not manage to
         *         report, meaning the fallback should run
         */
        boolean sawFailureWithoutDump() {
            return sawFailure.get() && !dumped.get();
        }

        @Override
        protected void append(ILoggingEvent event) {
            if (!event.getFormattedMessage().contains(ATTACH_FAILURE_MESSAGE)) {
                return;
            }
            sawFailure.set(true);

            // Only the first failure of a test is captured; a retry loop would otherwise produce ten
            // near-identical dumps.
            if (!dumped.compareAndSet(false, true)) {
                return;
            }
            try {
                TestHelper.logXStreamOutboundServerDiagnostics("at first failed attach during " + testName);
            }
            catch (Throwable t) {
                // Never allow a diagnostic to break the connector thread that happened to log the
                // failure; clearing the flag lets afterEach retry the capture.
                dumped.set(false);
                addError("Failed to collect XStream diagnostics inline", t);
            }
        }
    }
}

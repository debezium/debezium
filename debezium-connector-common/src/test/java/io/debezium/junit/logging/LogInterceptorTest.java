/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import static io.debezium.junit.logging.LogInterceptorTestUtils.attachedInterceptors;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import io.debezium.doc.FixFor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * Unit tests for the cleanup behavior of {@link LogInterceptor}.
 *
 * @author Chris Cranford
 */
public class LogInterceptorTest {

    private static final String PACKAGE = "io.debezium.junit.logging.interceptortest";

    private static final class InheritedTarget {
    }

    private static final class ExplicitTarget {
    }

    private final Logger inheritedLogger = (Logger) LoggerFactory.getLogger(InheritedTarget.class);
    private final Logger explicitLogger = (Logger) LoggerFactory.getLogger(ExplicitTarget.class);

    @AfterEach
    public void resetLevels() {
        // Runs before the cleanup extension, so clean up first in case a test failed before doing so,
        // and only then undo the level these tests set directly on the logger.
        LogInterceptorRegistry.cleanup();
        explicitLogger.setLevel(null);
    }

    @Test
    @FixFor("debezium/dbz#3062")
    public void shouldDetachFromClassLoggerOnClose() {
        final var interceptor = LogInterceptor.forClass(InheritedTarget.class);
        assertThat(attachedInterceptors(inheritedLogger)).containsExactly(interceptor);

        inheritedLogger.warn("before close");
        assertThat(interceptor.containsMessage("before close")).isTrue();

        interceptor.close();
        assertThat(attachedInterceptors(inheritedLogger)).isEmpty();
        assertThat(interceptor.isStarted()).isFalse();

        inheritedLogger.warn("after close");
        assertThat(interceptor.containsMessage("before close")).isFalse();
        assertThat(interceptor.containsMessage("after close")).isFalse();
    }

    @Test
    @FixFor("debezium/dbz#3062")
    public void shouldDetachFromAllPackageLoggersOnClose() {
        final var first = (Logger) LoggerFactory.getLogger(PACKAGE + ".First");
        final var second = (Logger) LoggerFactory.getLogger(PACKAGE + ".child.Second");

        final var interceptor = LogInterceptor.forPackage(PACKAGE);
        assertThat(attachedInterceptors(first)).containsExactly(interceptor);
        assertThat(attachedInterceptors(second)).containsExactly(interceptor);

        interceptor.close();
        assertThat(attachedInterceptors(first)).isEmpty();
        assertThat(attachedInterceptors(second)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#3062")
    public void shouldRestoreInheritedAndExplicitLevels() {
        explicitLogger.setLevel(Level.WARN);
        assertThat(inheritedLogger.getLevel()).isNull();

        final var interceptor = LogInterceptor.forClass(InheritedTarget.class);
        interceptor.setLoggerLevel(InheritedTarget.class, Level.DEBUG);
        interceptor.setLoggerLevel(ExplicitTarget.class, Level.TRACE);
        assertThat(inheritedLogger.getLevel()).isEqualTo(Level.DEBUG);
        assertThat(explicitLogger.getLevel()).isEqualTo(Level.TRACE);

        new LogInterceptorCleanupExtension().afterEach(null);

        assertThat(inheritedLogger.getLevel()).isNull();
        assertThat(explicitLogger.getLevel()).isEqualTo(Level.WARN);
        assertThat(attachedInterceptors(inheritedLogger)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#3062")
    public void shouldRestoreFirstOriginalLevelWhenChangedByTwoInterceptors() {
        explicitLogger.setLevel(Level.WARN);

        final var first = LogInterceptor.forClass(ExplicitTarget.class);
        final var second = LogInterceptor.forClass(ExplicitTarget.class);
        first.setLoggerLevel(ExplicitTarget.class, Level.INFO);
        second.setLoggerLevel(ExplicitTarget.class, Level.DEBUG);

        new LogInterceptorCleanupExtension().afterEach(null);

        assertThat(explicitLogger.getLevel()).isEqualTo(Level.WARN);
        assertThat(attachedInterceptors(explicitLogger)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#3062")
    public void shouldIgnoreRepeatedOrManualCleanup() {
        final var closedTwice = LogInterceptor.forClass(InheritedTarget.class);
        closedTwice.close();
        closedTwice.close();

        final var detachedManually = LogInterceptor.forClass(InheritedTarget.class);
        inheritedLogger.detachAppender(detachedManually);
        detachedManually.stop();
        detachedManually.close();

        new LogInterceptorCleanupExtension().afterEach(null);
        assertThat(attachedInterceptors(inheritedLogger)).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#3062")
    public void shouldRestoreLevelsChangedWithoutInterceptor() {
        explicitLogger.setLevel(Level.WARN);

        LogInterceptor.changeLoggerLevel(InheritedTarget.class.getName(), Level.DEBUG);
        LogInterceptor.changeLoggerLevel(ExplicitTarget.class.getName(), Level.TRACE);
        assertThat(inheritedLogger.getLevel()).isEqualTo(Level.DEBUG);
        assertThat(explicitLogger.getLevel()).isEqualTo(Level.TRACE);

        new LogInterceptorCleanupExtension().afterEach(null);

        assertThat(inheritedLogger.getLevel()).isNull();
        assertThat(explicitLogger.getLevel()).isEqualTo(Level.WARN);
    }

    @Test
    @FixFor("debezium/dbz#3062")
    public void shouldDoNothingWhenNothingWasRegistered() {
        LogInterceptorRegistry.cleanup();
        explicitLogger.setLevel(Level.WARN);

        new LogInterceptorCleanupExtension().afterEach(null);

        assertThat(explicitLogger.getLevel()).isEqualTo(Level.WARN);
    }

    @Test
    @FixFor("debezium/dbz#3062")
    public void shouldKeepCapturingUntilClosedWithTryWithResources() {
        try (var interceptor = LogInterceptor.forClass(InheritedTarget.class)) {
            inheritedLogger.warn("inside block");
            assertThat(interceptor.containsWarnMessage("inside block")).isTrue();
        }
        assertThat(attachedInterceptors(inheritedLogger)).isEmpty();
    }
}

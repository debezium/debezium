/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import static io.debezium.junit.logging.LogInterceptorTestUtils.attachedInterceptors;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.LoggerFactory;

import io.debezium.doc.FixFor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * Verifies that a {@link LogInterceptor} created in one test method does not leak its appender
 * or its logger level changes into later test methods running in the same JVM.
 * <p>
 * This checks that {@link LogInterceptorCleanupExtension} is registered through the
 * {@code META-INF/services} file, so it needs JUnit extension autodetection, which the build enables
 * with the {@code junit.jupiter.extensions.autodetection.enabled} system property. Without that
 * property, the second test is skipped rather than failed.
 *
 * @author Chris Cranford
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LogInterceptorLeakTest {

    private static final String AUTODETECTION_PROPERTY = "junit.jupiter.extensions.autodetection.enabled";

    /**
     * A class used only as a logger name, so no other test changes its level or appenders.
     */
    private static final class LeakTarget {
    }

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(LeakTarget.class);

    @Test
    @Order(1)
    @FixFor("debezium/dbz#3062")
    public void shouldAttachInterceptorAndRaiseLevel() {
        assertThat(LOGGER.getLevel()).isNull();

        final var interceptor = LogInterceptor.forClass(LeakTarget.class);
        interceptor.setLoggerLevel(LeakTarget.class, Level.DEBUG);

        assertThat(LOGGER.getLevel()).isEqualTo(Level.DEBUG);
        assertThat(attachedInterceptors(LOGGER)).containsExactly(interceptor);
    }

    @Test
    @Order(2)
    @FixFor("debezium/dbz#3062")
    public void shouldNotLeakInterceptorOrLevelIntoLaterTest() {
        Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty(AUTODETECTION_PROPERTY)),
                "JUnit extension autodetection is not enabled");

        assertThat(LOGGER.getLevel()).isNull();
        assertThat(attachedInterceptors(LOGGER)).isEmpty();
    }
}

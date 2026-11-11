/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import static io.debezium.junit.logging.LogInterceptorTestUtils.attachedInterceptors;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.LoggerFactory;

import io.debezium.doc.FixFor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * Verifies that a {@link LogInterceptor} held in a static field, and a logger level changed in {@code @BeforeAll},
 * stay in place across the test methods of its class, while an interceptor held in an instance field
 * and a level changed in a test method are undone after each test.
 *
 * @author Chris Cranford
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LogInterceptorStaticFieldTest {

    private static final String AUTODETECTION_PROPERTY = "junit.jupiter.extensions.autodetection.enabled";

    private static final class StaticTarget {
    }

    private static final class InstanceTarget {
    }

    private static final class LevelTarget {
    }

    private static final Logger STATIC_LOGGER = (Logger) LoggerFactory.getLogger(StaticTarget.class);
    private static final Logger INSTANCE_LOGGER = (Logger) LoggerFactory.getLogger(InstanceTarget.class);
    private static final Logger LEVEL_LOGGER = (Logger) LoggerFactory.getLogger(LevelTarget.class);

    private static final LogInterceptor staticInterceptor = LogInterceptor.forClass(StaticTarget.class);

    private final LogInterceptor instanceInterceptor = LogInterceptor.forClass(InstanceTarget.class);

    @BeforeAll
    public static void changeLevelForClass() {
        LogInterceptor.changeLoggerLevel(LevelTarget.class.getName(), Level.DEBUG);
    }

    @Test
    @Order(1)
    @FixFor("debezium/dbz#3062")
    public void shouldCaptureWithStaticInterceptorInFirstTest() {
        STATIC_LOGGER.warn("first test");
        assertThat(staticInterceptor.containsMessage("first test")).isTrue();
        assertThat(attachedInterceptors(INSTANCE_LOGGER)).containsExactly(instanceInterceptor);

        assertThat(LEVEL_LOGGER.getLevel()).isEqualTo(Level.DEBUG);
        LogInterceptor.changeLoggerLevel(LevelTarget.class.getName(), Level.TRACE);
    }

    @Test
    @Order(2)
    @FixFor("debezium/dbz#3062")
    public void shouldStillCaptureWithStaticInterceptorInLaterTest() {
        Assumptions.assumeTrue(Boolean.parseBoolean(System.getProperty(AUTODETECTION_PROPERTY)),
                "JUnit extension autodetection is not enabled");

        STATIC_LOGGER.warn("second test");
        assertThat(staticInterceptor.containsMessage("second test")).isTrue();
        assertThat(attachedInterceptors(STATIC_LOGGER)).containsExactly(staticInterceptor);
        assertThat(attachedInterceptors(INSTANCE_LOGGER)).containsExactly(instanceInterceptor);

        // The change made in the first test is undone, the one made for the class is kept.
        assertThat(LEVEL_LOGGER.getLevel()).isEqualTo(Level.DEBUG);
    }
}

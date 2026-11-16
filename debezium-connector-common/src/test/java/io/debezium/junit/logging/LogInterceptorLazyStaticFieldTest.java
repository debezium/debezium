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

import ch.qos.logback.classic.Logger;

/**
 * Verifies that a {@link LogInterceptor} held in a static field stays in place across the test methods of its
 * class when, with no static method invoked before, the class is only initialized as its first test instance is
 * constructed.
 *
 * @author Chris Cranford
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LogInterceptorLazyStaticFieldTest {

    private static final String AUTODETECTION_PROPERTY = "junit.jupiter.extensions.autodetection.enabled";

    private static final class StaticTarget {
    }

    private static final Logger STATIC_LOGGER = (Logger) LoggerFactory.getLogger(StaticTarget.class);

    private static final LogInterceptor staticInterceptor = LogInterceptor.forClass(StaticTarget.class);

    @Test
    @Order(1)
    @FixFor("debezium/dbz#3062")
    public void shouldCaptureWithStaticInterceptorInFirstTest() {
        STATIC_LOGGER.warn("first test");
        assertThat(staticInterceptor.containsMessage("first test")).isTrue();
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
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import java.util.ArrayList;
import java.util.List;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;

/**
 * Helpers shared by the {@link LogInterceptor} tests.
 *
 * @author Chris Cranford
 */
final class LogInterceptorTestUtils {

    private LogInterceptorTestUtils() {
    }

    /**
     * Returns the {@link LogInterceptor} instances currently attached to the given logger.
     *
     * @param logger the logger to inspect
     * @return the attached interceptors, never {@code null}
     */
    static List<LogInterceptor> attachedInterceptors(Logger logger) {
        final List<LogInterceptor> interceptors = new ArrayList<>();
        logger.iteratorForAppenders().forEachRemaining((Appender<ILoggingEvent> appender) -> {
            if (appender instanceof LogInterceptor interceptor) {
                interceptors.add(interceptor);
            }
        });
        return interceptors;
    }
}

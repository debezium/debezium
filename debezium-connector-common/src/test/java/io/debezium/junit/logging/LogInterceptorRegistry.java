/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.logging;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * Tracks the loggers each {@link LogInterceptor} is attached to, and the original level of every logger
 * whose level was changed through {@link LogInterceptor#setLoggerLevel(Class, Level)} or
 * {@link LogInterceptor#changeLoggerLevel(String, Level)}, so that both can be
 * undone when a test ends.
 *
 * @author Chris Cranford
 */
final class LogInterceptorRegistry {

    private static final Map<LogInterceptor, Set<Logger>> ATTACHMENTS = new ConcurrentHashMap<>();

    // A null value means the logger inherited its level, so a map that accepts null values is required.
    private static final Map<Logger, Level> ORIGINAL_LEVELS = new LinkedHashMap<>();

    private LogInterceptorRegistry() {
    }

    static void registerAttachment(LogInterceptor interceptor, Logger logger) {
        ATTACHMENTS.computeIfAbsent(interceptor, key -> ConcurrentHashMap.newKeySet()).add(logger);
    }

    static Set<Logger> unregister(LogInterceptor interceptor) {
        final var loggers = ATTACHMENTS.remove(interceptor);
        return loggers != null ? loggers : Set.of();
    }

    static void recordOriginalLevel(Logger logger) {
        synchronized (ORIGINAL_LEVELS) {
            if (!ORIGINAL_LEVELS.containsKey(logger)) {
                ORIGINAL_LEVELS.put(logger, logger.getLevel());
            }
        }
    }

    /**
     * Restores the original level of every logger changed through an interceptor, then detaches and
     * stops every interceptor that is still attached.
     * <p>
     * When nothing was registered this returns before touching any logback type, so the cleanup extension
     * also works in modules that use the test-jar without having logback on their test classpath.
     */
    static void cleanup() {
        final Map<Logger, Level> levels;
        synchronized (ORIGINAL_LEVELS) {
            if (ORIGINAL_LEVELS.isEmpty() && ATTACHMENTS.isEmpty()) {
                return;
            }
            levels = new LinkedHashMap<>(ORIGINAL_LEVELS);
            ORIGINAL_LEVELS.clear();
        }
        levels.forEach(Logger::setLevel);

        final List<LogInterceptor> interceptors = new ArrayList<>(ATTACHMENTS.keySet());
        interceptors.forEach(LogInterceptor::close);
    }
}

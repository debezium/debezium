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
import java.util.concurrent.atomic.AtomicLong;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * Tracks the loggers each {@link LogInterceptor} is attached to, and the original level of every logger
 * whose level was changed through {@link LogInterceptor#setLoggerLevel(Class, Level)} or
 * {@link LogInterceptor#changeLoggerLevel(String, Level)}, so that both can be
 * undone when a test ends.
 * <p>
 * Every interceptor and every level change is stamped with an increasing sequence number when it is first
 * registered, so that {@link #cleanup(long)} can undo only what was registered after a given {@link #mark()},
 * leaving alone, for example, interceptors held in static fields or created in {@code @BeforeAll}.
 *
 * @author Chris Cranford
 */
final class LogInterceptorRegistry {

    private record OriginalLevel(Level level, long sequence) {
    }

    private static final AtomicLong SEQUENCE = new AtomicLong();

    private static final Map<LogInterceptor, Set<Logger>> ATTACHMENTS = new ConcurrentHashMap<>();
    private static final Map<LogInterceptor, Long> REGISTERED_AT = new ConcurrentHashMap<>();

    // For each logger, the level it had when it was first changed in each scope, oldest scope first.
    private static final Map<Logger, List<OriginalLevel>> ORIGINAL_LEVELS = new LinkedHashMap<>();

    // The marker at which the current scope, a test class or a test method, started.
    private static volatile long scopeStart;

    private LogInterceptorRegistry() {
    }

    /**
     * Starts a new scope at the given marker, so that the next level change of a logger already changed
     * in an outer scope records the level the logger has at that point and restores it when this scope is cleaned up.
     *
     * @param mark a value returned by {@link #mark()}
     */
    static void enterScope(long mark) {
        scopeStart = mark;
    }

    static void registerAttachment(LogInterceptor interceptor, Logger logger) {
        REGISTERED_AT.computeIfAbsent(interceptor, key -> SEQUENCE.incrementAndGet());
        ATTACHMENTS.computeIfAbsent(interceptor, key -> ConcurrentHashMap.newKeySet()).add(logger);
    }

    static Set<Logger> unregister(LogInterceptor interceptor) {
        REGISTERED_AT.remove(interceptor);
        final var loggers = ATTACHMENTS.remove(interceptor);
        return loggers != null ? loggers : Set.of();
    }

    static void recordOriginalLevel(Logger logger) {
        synchronized (ORIGINAL_LEVELS) {
            final var entries = ORIGINAL_LEVELS.computeIfAbsent(logger, key -> new ArrayList<>());
            if (entries.isEmpty() || entries.get(entries.size() - 1).sequence() <= scopeStart) {
                entries.add(new OriginalLevel(logger.getLevel(), SEQUENCE.incrementAndGet()));
            }
        }
    }

    /**
     * Returns a marker that can later be passed to {@link #cleanup(long)} to undo only what is registered after it.
     */
    static long mark() {
        return SEQUENCE.get();
    }

    /**
     * Restores the original level of every logger changed through an interceptor, then detaches and
     * stops every interceptor that is still attached.
     */
    static void cleanup() {
        cleanup(0L);
    }

    /**
     * Restores the original level of every logger first changed after the given marker, then detaches and
     * stops every interceptor registered after it that is still attached.
     * <p>
     * When nothing was registered this returns before touching any logback type, so the cleanup extension
     * also works in modules that use the test-jar without having logback on their test classpath.
     *
     * @param since a value returned by {@link #mark()}, or {@code 0} to clean up everything
     */
    static void cleanup(long since) {
        final Map<Logger, Level> levels = new LinkedHashMap<>();
        synchronized (ORIGINAL_LEVELS) {
            if (ORIGINAL_LEVELS.isEmpty() && ATTACHMENTS.isEmpty()) {
                return;
            }
            final var iterator = ORIGINAL_LEVELS.entrySet().iterator();
            while (iterator.hasNext()) {
                final var entry = iterator.next();
                final var entries = entry.getValue();
                // Entries are ordered by sequence, so drop the newest ones and restore the oldest of those.
                while (!entries.isEmpty() && entries.get(entries.size() - 1).sequence() > since) {
                    levels.put(entry.getKey(), entries.remove(entries.size() - 1).level());
                }
                if (entries.isEmpty()) {
                    iterator.remove();
                }
            }
        }
        levels.forEach(Logger::setLevel);

        final List<LogInterceptor> interceptors = new ArrayList<>();
        REGISTERED_AT.forEach((interceptor, sequence) -> {
            if (sequence > since) {
                interceptors.add(interceptor);
            }
        });
        interceptors.forEach(LogInterceptor::close);
    }
}

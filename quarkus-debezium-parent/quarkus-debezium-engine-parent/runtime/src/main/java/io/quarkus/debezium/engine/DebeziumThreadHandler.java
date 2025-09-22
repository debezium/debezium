/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.CaptureGroup;
import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumContext;

public class DebeziumThreadHandler {
    private static final InheritableThreadLocal<DebeziumContext> context = new InheritableThreadLocal<>();
    private static final Map<CaptureGroup, AtomicInteger> captureGroups = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumThreadHandler.class.getName());

    /**
     * Generate a {@link ThreadFactory} and initialize the {@link DebeziumContext} for a give {@link Debezium} engine
     * @param debezium engine that use generated {@link ThreadFactory}
     * @return the {@link ThreadFactory} customized for Quarkus
     */
    static ThreadFactory getThreadFactory(Debezium debezium) {
        return runnable -> {
            int num = captureGroups
                    .computeIfAbsent(debezium.captureGroup(), ignore -> new AtomicInteger(0))
                    .incrementAndGet();

            captureGroups.put(debezium.captureGroup(), new AtomicInteger(num));

            return new Thread(() -> {
                context.set(debezium::captureGroup);
                runnable.run();
            }, "dbz-" + debezium.captureGroup().id() + "-" + num);
        };
    }

    /**
     * The context is created through the dev/prod flow of the extension with {@link #getThreadFactory(Debezium)}. If
     * it's called without starting the Quarkus Application (like during integration tests) the context is initialized
     * with a dummy context.
     * @return the {@link DebeziumContext} associated to the running engine
     */
    public static DebeziumContext context() {
        if (context.get() == null) {

            LOGGER.warn("Debezium context not initialized, using testing context");
            return () -> new CaptureGroup("testing");
        }
        return context.get();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.Debezium;
import io.debezium.runtime.DebeziumContext;
import io.debezium.runtime.EngineManifest;
import io.debezium.util.Threads;

public class DebeziumThreadHandler {
    private static final InheritableThreadLocal<DebeziumContext> context = new InheritableThreadLocal<>();
    private static final Map<EngineManifest, Integer> manifests = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumThreadHandler.class.getName());
    public static final String COMPONENT = "quarkus-extension";

    /**
     * Generate a {@link ThreadFactory} and initialize the {@link DebeziumContext} for a give {@link Debezium} engine
     * @param debezium engine that use generated {@link ThreadFactory}
     * @return the {@link ThreadFactory} customized for Quarkus
     */
    static ThreadFactory getThreadFactory(Debezium debezium) {
        return Threads.threadFactory(debezium.connector().getClass(),
                COMPONENT,
                debezium.manifest().id(),
                true,
                false, null, Optional.of(() -> context.set(debezium::manifest)));
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
            return () -> new EngineManifest("testing");
        }
        return context.get();
    }
}

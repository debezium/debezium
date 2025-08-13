/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.runtime.configuration;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

/**
 * Debezium configuration.
 */
@ConfigMapping(prefix = "quarkus")
@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
public interface DebeziumEngineConfiguration {
    /**
     * Configuration properties for debezium engine
     */
    @WithName("debezium")
    Map<String, String> configuration();

    /**
     * Configuration for capturing events
     */
    @WithName("debezium.capturing")
    Map<String, Capturing> capturing();

    interface Capturing {
        /**
         * destination for which the event is intended
         */
        Optional<String> destination();

        /**
         * deserializer class for the event associated to a destination
         */
        Optional<String> deserializer();
    }
}

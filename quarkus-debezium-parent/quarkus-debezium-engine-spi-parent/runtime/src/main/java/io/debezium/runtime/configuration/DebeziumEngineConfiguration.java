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
import io.smallrye.config.WithParentName;

/**
 * Debezium configuration.
 */
@ConfigMapping(prefix = "quarkus")
@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
public interface DebeziumEngineConfiguration {
    /**
     * Default Configuration properties for debezium engine
     */
    @WithName("debezium")
    Map<String, String> defaultConfiguration();

    /**
     * Configuration for capturing events
     */
    @WithName("debezium.capturing")
    Map<String, Capturing> capturing();

    interface Capturing {

        /**
         * identify the capturer group assigned to a datasource
         */
        Optional<String> groupId();

        /**
         * destination for which the event is intended
         */
        Optional<String> destination();

        /**
         * deserializers in a multi-engine configuration
         */
        Optional<String> deserializer();

        /**
         * blablabla
         */
        @WithParentName
        Map<String, DeserializerConfiguration> deserializers();

        /**
         * configuration properties for debezium multi-engine
         */
        @WithParentName
        Map<String, String> configurations();
    }

    /**
     * deserializer configuration
     */
    interface DeserializerConfiguration {
        /**
         * destination for which the event is intended
         */
        String destination();

        /**
         * deserializer class for the event associated to a destination
         */
        String deserializer();
    }
}

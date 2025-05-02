/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.debezium.configuration;

import java.util.Map;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;

/**
 * Debezium configuration.
 */
@ConfigMapping(prefix = "quarkus.debezium")
@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
public interface DebeziumEngineConfiguration {
    /**
     * Configuration properties
     */
    Map<String, String> configuration();
}

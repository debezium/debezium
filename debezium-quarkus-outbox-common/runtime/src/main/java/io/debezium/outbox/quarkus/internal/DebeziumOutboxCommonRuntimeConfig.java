/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Debezium outbox Quarkus extension common runtime configuration properties.
 *
 * @author Chris Cranford
 */
@ConfigMapping(prefix = "quarkus.debezium-outbox")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface DebeziumOutboxCommonRuntimeConfig {
    /**
     * Remove outbox entity after being inserted.  Default is {@code true}.
     */
    @WithDefault("true")
    boolean removeAfterInsert();
}

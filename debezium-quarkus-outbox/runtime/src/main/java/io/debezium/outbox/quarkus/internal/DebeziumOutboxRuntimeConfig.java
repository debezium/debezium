/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.quarkus.internal;

import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

/**
 * Debezium outbox Quarkus extension runtime configuration properties.
 *
 * @author Chris Cranford
 */
@ConfigRoot(phase = ConfigPhase.RUN_TIME, name = "debezium-outbox")
public class DebeziumOutboxRuntimeConfig {
    /**
     * Remove outbox entity after being inserted.  Default is {@code true}.
     */
    @ConfigItem(defaultValue = "true")
    public boolean removeAfterInsert;
}

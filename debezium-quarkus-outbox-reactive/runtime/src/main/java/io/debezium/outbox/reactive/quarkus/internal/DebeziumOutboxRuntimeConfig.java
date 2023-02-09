/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.reactive.quarkus.internal;

import io.debezium.outbox.quarkus.internal.DebeziumOutboxCommonRuntimeConfig;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

/**
 * Debezium outbox Quarkus extension runtime configuration properties.
 *
 * @author Chris Cranford
 */
@ConfigRoot(phase = ConfigPhase.RUN_TIME, name = "debezium-outbox")
public class DebeziumOutboxRuntimeConfig extends DebeziumOutboxCommonRuntimeConfig {

}

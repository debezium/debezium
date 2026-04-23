/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import io.debezium.common.annotation.Incubating;

/**
 * Internal handler responsible for evaluating records against shutdown conditions
 * and triggering engine shutdown when conditions are met. This is an implementation
 * detail of the {@link DebeziumEngine.ShutdownBuilder} API.
 *
 * <p>A shutdown handler wraps a {@link io.debezium.engine.DebeziumEngine.ShutdownStrategy}
 * and is inserted into the record processing pipeline to evaluate each record as it
 * flows through. Handlers can be positioned either before or after the consumer processes
 * records, depending on the shutdown configuration.
 *
 *
 * @see DefaultShutdownHandler
 * @see ShutdownChangeConsumer
 * @see ShutdownConsumer
 * @see io.debezium.engine.DebeziumEngine.ShutdownBuilder
 * @see io.debezium.engine.DebeziumEngine.ShutdownStrategy
 * @since 3.6.0
 */
@Incubating
public interface ShutdownHandler<R> {
    /**
     * Evaluates a single record against the shutdown condition. This method is called
     * for each record as it flows through the processing pipeline.
     *
     * <p><strong>Performance:</strong> This method is called for every record, so implementations
     * should be lightweight and avoid expensive operations.
     *
     * @param record the record to evaluate; never null
     */
    void evaluate(R record);
}

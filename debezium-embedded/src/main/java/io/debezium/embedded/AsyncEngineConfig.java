/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import io.debezium.config.Field;

/**
 * Configuration options specific to {@link AsyncEmbeddedEngine}.
 *
 * @author vjuranek
 */
public interface AsyncEngineConfig extends EmbeddedEngineConfig {

    int AVAILABLE_CORES = Runtime.getRuntime().availableProcessors();
    int RECORD_PROCESSING_THREADS_CAP = 16;

    /**
     * An optional field that specifies the number of threads to be used for processing CDC records.
     */
    Field RECORD_PROCESSING_THREADS = Field.create("record.processing.threads")
            .withDescription("The number of threads to be used for processing CDC records. The default is number of available machine cores with upper "
                    + "limit of " + RECORD_PROCESSING_THREADS_CAP + " threads. If you want to use all available thread without any limitation, use 'AVAILABLE_CORES' "
                    + "placeholder.")
            .withDefault(AVAILABLE_CORES);

    /**
     * An optional field that specifies maximum time in ms to wait for submitted records to finish processing when the task shut down is called.
     */
    Field RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS = Field.create("record.processing.shutdown.timeout.ms")
            .withDescription("Maximum time in milliseconds to wait for processing submitted records when task shutdown is called. The default is 10 seconds (10000 ms).")
            .withDefault(1000L)
            .withValidation(Field::isPositiveInteger);

    /**
     * An optional field that specifies how the records will be produced. Sequential processing (the default) means that the records will be produced in the same order
     * as the engine obtained them from the connector. Non-sequential processing means that the records can be produced in arbitrary order, typically once the record is
     * transformed and/or serialized.
     * This option doesn't have any effect when {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} is provided to the engine. In such case the records are always
     * processed sequentially.
     */
    Field RECORD_PROCESSING_ORDER = Field.create("record.processing.order")
            .withDescription("Determines how the records should be produced. "
                    + "'ORDERED' (the default) means sequential processing, i.e. that the records are produced in the same order as they were obtained from the database. "
                    + "'UNORDERED' means non-sequential processing, i.e. the records can be produced in a different order than the original one. "
                    + "Non-sequential approach gives better throughput, as the records are produced immediately once the SMTs and serialization of "
                    + "the message is done, without waiting of other records. This option doesn't have any effect when ChangeConsumer is provided to the engine.")
            .withDefault("ORDERED");

    /**
     * An optional field that specifies if the default {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} should be created for consuming records or not.
     * If only {@link java.util.function.Consumer} is provided to the engine and this option is set to {@code true} (the default is {@code false}), engine will create default
     * {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} and use it for record processing. Default {@link io.debezium.engine.DebeziumEngine.ChangeConsumer}
     * implementation is taken from legacy {@link EmbeddedEngine}, so this option allows to use almost the same implementation for record processing as {@link EmbeddedEngine}.
     * The only difference to {@link EmbeddedEngine} is that SMTs will be still run in parallel, even when this option is turned on.
     * This option doesn't have any effect when {@link io.debezium.engine.DebeziumEngine.ChangeConsumer} is already provided to the engine in the configuration.
     */
    Field RECORD_PROCESSING_WITH_SERIAL_CONSUMER = Field.create("record.processing.with.serial.consumer")
            .withDescription("Specifies whether the default ChangeConsumer should be created from provided Consumer, resulting in serial Consumer processing. "
                    + "This option has no effect if the ChangeConsumer is already provided to the engine via configuration.")
            .withDefault(false)
            .withValidation(Field::isBoolean);

    /**
     * An internal field that specifies the maximum amount of time to wait for a task lifecycle operation, i.e. for starting and stopping the task.
     */
    Field TASK_MANAGEMENT_TIMEOUT_MS = Field.createInternal("task.management.timeout.ms")
            .withDescription("Time to wait for task's lifecycle management operations (starting and stopping), given in milliseconds. "
                    + "Defaults to 2 minutes (120_000 ms).")
            .withDefault(120_000L)
            .withValidation(Field::isPositiveInteger);

    /**
     * The array of all exposed fields.
     */
    Field.Set ALL_FIELDS = EmbeddedEngineConfig.ALL_FIELDS.with(
            RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS,
            RECORD_PROCESSING_THREADS,
            RECORD_PROCESSING_ORDER,
            RECORD_PROCESSING_WITH_SERIAL_CONSUMER,
            // internal fields
            TASK_MANAGEMENT_TIMEOUT_MS);
}

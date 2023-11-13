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

    /**
     * An optional field that specifies the maximum amount of time to wait for task lifecycle operation, i.e. for starting and stopping the task.
     */
    Field TASK_MANAGEMENT_TIMEOUT_MS = Field.create("task.management.timeout.ms")
            .withDescription("Time to wait for task's lifecycle management operations (starting and stopping), given in milliseconds. "
                    + "Defaults to 5 seconds (5000 ms).")
            .withDefault(5_000L)
            .withValidation(Field::isPositiveInteger);

    /**
     * An optional field that specifies the number of threads to be used for processing CDC records.
     */
    Field RECORD_PROCESSING_THREADS = Field.create("record.processing.threads")
            .withDescription("The number of threads to be used for processing CDC records. The default is number of available machine cores.")
            .withDefault(AVAILABLE_CORES)
            .withValidation(Field::isPositiveInteger);

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
     */
    Field RECORD_PROCESSING_SEQUENTIALLY = Field.create("record.processing.sequentially")
            .withDescription("Determines how the records should be produce. Sequential processing means (setting to `true`, the default) that the records are "
                    + "produced in the same order as they were obtained from the database. Non-sequential processing means that the records can be produced in a different order "
                    + "order than the original one, but this approach given better throughput, as the records are produced immediately once the SMTs and serialization of "
                    + "the message is done, without waiting of other records.")
            .withDefault(true)
            .withValidation(Field::isBoolean);

    /**
     * The array of all exposed fields.
     */
    Field.Set ALL_FIELDS = EmbeddedEngineConfig.ALL_FIELDS.with(
            TASK_MANAGEMENT_TIMEOUT_MS,
            RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS,
            RECORD_PROCESSING_THREADS,
            RECORD_PROCESSING_SEQUENTIALLY);
}

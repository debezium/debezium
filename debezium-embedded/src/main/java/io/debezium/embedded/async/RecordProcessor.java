/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.common.annotation.Incubating;
import io.debezium.embedded.Transformations;
import io.debezium.engine.DebeziumEngine;

/**
 * Generalization of {@link DebeziumEngine.ChangeConsumer}, giving the user complete control over the whole records processing chain.
 * Processor is initialized with all the required engine internals, like chain of transformations, to be able to implement whole record processing chain.
 * Implementations can provide serial or parallel processing of the change records or anything in between, eventually also add any other kind of manipulation
 * with the records.
 * Any exception thrown during the processing of the records it propagated to the caller.
 */
@Incubating
public interface RecordProcessor<R> {

    /**
     * Initialize the processor with objects created and managed by {@link DebeziumEngine}, which are needed for records processing.
     *
     * @param recordService {@link ExecutorService} which allows to run processing of individual records in parallel.
     * @param transformations chain of transformations to be applied on every individual record.
     * @param committer implementation of {@link DebeziumEngine.RecordCommitter} responsible for committing individual records as well as batches.
     */
    void initialize(ExecutorService recordService, Transformations transformations, DebeziumEngine.RecordCommitter committer);

    /**
     * Processes a batch of records provided by the source connector.
     * Implementations are assumed to use {@link DebeziumEngine.RecordCommitter} to appropriately commit individual records and the batch itself.
     *
     * @param records List of {@link SourceRecord} provided by the source connector to be processed.
     * @throws Exception Any exception is propagated to the caller.
     */
    void processRecords(List<SourceRecord> records) throws Exception;
}

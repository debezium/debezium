/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.DebeziumEngine;

/**
 * {@link RecordProcessor} which runs transformations of the records in parallel and then pass the whole batch to the user-provided handler.
 * This processor should be used when user provides its own {@link DebeziumEngine.ChangeConsumer} and records shouldn't be converted to different format.
 *
 * @author vjuranek
 */
public class ParallelSmtBatchProcessor extends AbstractRecordProcessor<SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtBatchProcessor.class);

    final DebeziumEngine.RecordCommitter committer;
    final DebeziumEngine.ChangeConsumer<SourceRecord> userHandler;

    ParallelSmtBatchProcessor(final DebeziumEngine.RecordCommitter committer, final DebeziumEngine.ChangeConsumer<SourceRecord> userHandler) {
        this.committer = committer;
        this.userHandler = userHandler;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
        final List<Future<SourceRecord>> recordFutures = new ArrayList<>(records.size());
        records.stream().forEachOrdered(r -> recordFutures.add(recordService.submit(new ProcessingCallables.TransformRecord(r, transformations))));

        LOGGER.trace("Thread {} is getting source records.", Thread.currentThread().getName());
        final List<SourceRecord> transformedRecords = new ArrayList<>(recordFutures.size());
        for (Future<SourceRecord> f : recordFutures) {
            SourceRecord record = f.get(); // we need the whole batch, eventually wait forever
            if (record != null) {
                transformedRecords.add(record);
            }
        }

        LOGGER.trace("Calling user handler.");
        userHandler.handleBatch(transformedRecords, committer);
    }
}

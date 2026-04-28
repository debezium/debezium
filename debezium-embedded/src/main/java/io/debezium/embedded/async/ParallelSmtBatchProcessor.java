/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import io.debezium.embedded.async.AbstractRecordProcessor.BatchProcessor.ObservableProcessor;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Watcher;

/**
 * {@link RecordProcessor} which runs transformations of the records in parallel and then pass the whole batch to the user-provided handler.
 * This processor should be used when user provides its own {@link DebeziumEngine.ChangeConsumer} and records shouldn't be converted to different format.
 *
 * @author vjuranek
 */
public class ParallelSmtBatchProcessor extends AbstractRecordProcessor<SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtBatchProcessor.class);

    private final BatchProcessor<SourceRecord> processor;

    public ParallelSmtBatchProcessor(BatchProcessor<SourceRecord> processor) {
        this.processor = processor;
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

        processor.process(transformedRecords);
    }

    public static <R> ParallelSmtBatchProcessor create(DebeziumEngine.RecordCommitter<SourceRecord> committer,
                                                   final DebeziumEngine.ChangeConsumer<R> userHandler,
                                                   Watcher watcher,
                                                       DebeziumShutdown<R> shutdown,
                                                   Runnable runner) {

        if (shutdown == null) {
            return new ParallelSmtBatchProcessor(new BatchProcessor.DirectProcessor(committer, userHandler));
        }

        return new ParallelSmtBatchProcessor(
                new ObservableProcessor(
                        watcher,
                        committer,
                        new ShutdownChangeConsumer<>(
                                (ShutdownHandler<SourceRecord>) DefaultShutdownHandler.create(shutdown.before(), runner, committer),
                                (ShutdownHandler<SourceRecord>) DefaultShutdownHandler.create(shutdown.after(), runner, committer),
                                (DebeziumEngine.ChangeConsumer<SourceRecord>) userHandler)));
    }
}

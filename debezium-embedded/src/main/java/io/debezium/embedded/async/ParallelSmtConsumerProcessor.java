/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordProcessor} which transforms the records in parallel. Records are passed to the user-provided {@link Consumer}.
 * This processor should be used when user provides only custom {@link Consumer} and records should be passed without converting to the consumer in the same
 * order as they were obtained from the database.
 *
 * @author vjuranek
 */
public class ParallelSmtConsumerProcessor<R> extends AbstractRecordProcessor<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtConsumerProcessor.class);

    final Consumer<SourceRecord> consumer;

    ParallelSmtConsumerProcessor(final Consumer<SourceRecord> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
        final List<Future<SourceRecord>> recordFutures = new ArrayList<>(records.size());
        records.stream().forEachOrdered(r -> recordFutures.add(recordService.submit(new ProcessingCallables.TransformRecord(r, transformations))));

        LOGGER.trace("Waiting for the batch to finish processing.");
        final List<SourceRecord> transformedRecords = new ArrayList<>(recordFutures.size());
        for (Future<SourceRecord> f : recordFutures) {
            transformedRecords.add(f.get()); // we need the whole batch, eventually wait forever
        }

        LOGGER.trace("Calling user consumer.");
        for (int i = 0; i < records.size(); i++) {
            consumer.accept(transformedRecords.get(i));
            committer.markProcessed(records.get(i));
        }

        LOGGER.trace("Marking batch as finished.");
        committer.markBatchFinished();
    }
}
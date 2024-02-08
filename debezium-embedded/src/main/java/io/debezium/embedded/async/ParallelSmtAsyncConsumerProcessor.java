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
 * {@link RecordProcessor} which transforms the records in parallel. Records are passed to the user-provided {@link Consumer} in arbitrary order, once they are
 * processed. This processor should be used when user provides only custom {@link Consumer} and records should be passed without converting to the consumer in the same
 * order as they were obtained from the database.
 *
 * @author vjuranek
 */
public class ParallelSmtAsyncConsumerProcessor<R> extends AbstractRecordProcessor<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtAsyncConsumerProcessor.class);

    final Consumer<SourceRecord> consumer;

    ParallelSmtAsyncConsumerProcessor(final Consumer<SourceRecord> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
        final List<Future<Void>> recordFutures = new ArrayList<>(records.size());
        records.stream().forEachOrdered(r -> recordFutures.add(recordService.submit(new ProcessingCallables.TransformAndConsumeRecord(r, transformations, consumer))));

        LOGGER.trace("Waiting for the batch to finish processing.");
        for (int i = 0; i < records.size(); i++) {
            recordFutures.get(i);
            committer.markProcessed(records.get(i));
        }

        LOGGER.trace("Marking batch as finished.");
        committer.markBatchFinished();
    }
}
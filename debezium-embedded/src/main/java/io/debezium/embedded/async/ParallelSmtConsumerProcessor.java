/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.DebeziumEngine;

/**
 * {@link RecordProcessor} which transforms the records in parallel. Records are passed to the user-provided {@link Consumer}.
 * This processor should be used when user provides only custom {@link Consumer} and records should be passed without converting to the consumer in the same
 * order as they were obtained from the database.
 *
 * @author vjuranek
 */
public class ParallelSmtConsumerProcessor extends AbstractRecordProcessor<SourceRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtConsumerProcessor.class);

    final DebeziumEngine.RecordCommitter committer;
    final Consumer<SourceRecord> consumer;

    ParallelSmtConsumerProcessor(final DebeziumEngine.RecordCommitter committer, final Consumer<SourceRecord> consumer) {
        this.committer = committer;
        this.consumer = consumer;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
        final Future<SourceRecord>[] recordFutures = new Future[records.size()];
        int i = 0;
        for (SourceRecord r : records) {
            recordFutures[i] = recordService.submit(new ProcessingCallables.TransformRecord(r, transformations));
            i++;
        }

        LOGGER.trace("Calling user consumer.");
        i = 0;
        for (SourceRecord r : records) {
            consumer.accept(recordFutures[i].get());
            committer.markProcessed(r);
            i++;
        }

        LOGGER.trace("Marking batch as finished.");
        committer.markBatchFinished();
    }
}

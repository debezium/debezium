/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.DebeziumEngine;

/**
 * {@link RecordProcessor} which transforms and converts the records in parallel. Records are passed to the user-provided {@link Consumer} in arbitrary order, once
 * they are processed. This processor should be used when user provides only custom {@link Consumer}, records should be converted and passed to the consumer in
 * arbitrary order.
 *
 * @author vjuranek
 */
public class ParallelSmtAndConvertAsyncConsumerProcessor<R> extends AbstractRecordProcessor<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtAndConvertAsyncConsumerProcessor.class);

    final DebeziumEngine.RecordCommitter committer;
    final Consumer<R> consumer;
    final Function<SourceRecord, R> convertor;

    ParallelSmtAndConvertAsyncConsumerProcessor(DebeziumEngine.RecordCommitter committer, final Consumer<R> consumer, final Function<SourceRecord, R> convertor) {
        this.committer = committer;
        this.consumer = consumer;
        this.convertor = convertor;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());

        final Future<Void>[] recordFutures = new Future[records.size()];
        Iterator<SourceRecord> recordsIterator = records.iterator();
        for (int i = 0; recordsIterator.hasNext(); i++) {
            recordFutures[i] = recordService
                    .submit(new ProcessingCallables.TransformConvertConsumeRecord<>(recordsIterator.next(), transformations, convertor, consumer));
        }

        LOGGER.trace("Waiting for the batch to finish processing.");
        recordsIterator = records.iterator();
        for (int i = 0; recordsIterator.hasNext(); i++) {
            recordFutures[i].get();
            committer.markProcessed(recordsIterator.next());
        }

        LOGGER.trace("Marking batch as finished.");
        committer.markBatchFinished();
    }
}

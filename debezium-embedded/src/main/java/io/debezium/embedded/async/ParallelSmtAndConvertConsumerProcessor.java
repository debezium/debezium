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
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordProcessor} which transforms and converts the records in parallel. Converted records are passed to the user-provided {@link Consumer}.
 * This processor should be used when user provides only custom {@link Consumer}, records should be converted and passed to the consumer in the same order as they
 * were obtained from the database.
 *
 * @author vjuranek
 */
public class ParallelSmtAndConvertConsumerProcessor<R> extends AbstractRecordProcessor<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtAndConvertConsumerProcessor.class);

    final Consumer<R> consumer;
    final Function<SourceRecord, R> convertor;

    ParallelSmtAndConvertConsumerProcessor(final Consumer<R> consumer, final Function<SourceRecord, R> convertor) {
        this.consumer = consumer;
        this.convertor = convertor;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
        final List<Future<R>> recordFutures = new ArrayList<>(records.size());
        records.stream().forEachOrdered(r -> recordFutures.add(recordService.submit(new ProcessingCallables.TransformAndConvertRecord(r, transformations, convertor))));

        LOGGER.trace("Waiting for the batch to finish processing.");
        final List<R> convertedRecords = new ArrayList<>(recordFutures.size());
        for (Future<R> f : recordFutures) {
            convertedRecords.add(f.get()); // we need the whole batch, eventually wait forever
        }

        LOGGER.trace("Calling user consumer.");
        for (int i = 0; i < records.size(); i++) {
            consumer.accept(convertedRecords.get(i));
            committer.markProcessed(records.get(i));
        }

        LOGGER.trace("Marking batch as finished.");
        committer.markBatchFinished();
    }
}
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
 * {@link RecordProcessor} which transforms and converts the records in parallel. Records are passed to the user-provided {@link Consumer} in arbitrary order, once
 * they are processed. This processor should be used when user provides only custom {@link Consumer}, records should be converted and passed to the consumer in
 * arbitrary order.
 *
 * @author vjuranek
 */
public class ParallelSmtAndConvertAsyncConsumerProcessor<R> extends AbstractRecordProcessor<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtAndConvertAsyncConsumerProcessor.class);

    final Consumer<R> consumer;
    final Function<SourceRecord, R> convertor;

    ParallelSmtAndConvertAsyncConsumerProcessor(final Consumer<R> consumer, final Function<SourceRecord, R> convertor) {
        this.consumer = consumer;
        this.convertor = convertor;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
        final List<Future<Void>> recordFutures = new ArrayList<>(records.size());
        records.stream().forEachOrdered(
                r -> recordFutures.add(recordService.submit(new ProcessingCallables.TransformConvertConsumeRecord<>(r, transformations, convertor, consumer))));

        LOGGER.trace("Waiting for the batch to finish processing.");
        for (int i = 0; i < records.size(); i++) {
            recordFutures.get(i);
            committer.markProcessed(records.get(i));
        }

        LOGGER.trace("Marking batch as finished.");
        committer.markBatchFinished();
    }
}
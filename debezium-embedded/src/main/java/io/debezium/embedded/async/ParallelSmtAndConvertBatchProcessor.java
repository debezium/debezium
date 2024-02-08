/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.DebeziumEngine;

/**
 * {@link RecordProcessor} which transforms and converts the records in parallel and then pass the whole batch to the user-provided handler.
 * This processor should be used when user provides its own {@link DebeziumEngine.ChangeConsumer} and records should be converted to different format.
 *
 * @author vjuranek
 */
public class ParallelSmtAndConvertBatchProcessor<R> extends AbstractRecordProcessor<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelSmtAndConvertBatchProcessor.class);

    final DebeziumEngine.ChangeConsumer<R> userHandler;
    final Function<SourceRecord, R> convertor;

    ParallelSmtAndConvertBatchProcessor(final DebeziumEngine.ChangeConsumer<R> userHandler, final Function<SourceRecord, R> convertor) {
        this.userHandler = userHandler;
        this.convertor = convertor;
    }

    @Override
    public void processRecords(final List<SourceRecord> records) throws Exception {
        LOGGER.debug("Thread {} is submitting {} records for processing.", Thread.currentThread().getName(), records.size());
        final List<Future<R>> recordFutures = new ArrayList<>(records.size());
        records.stream()
                .forEachOrdered(r -> recordFutures.add(recordService.submit(new ProcessingCallables.TransformAndConvertRecord<R>(r, transformations, convertor))));

        LOGGER.trace("Getting source records.");
        final List<R> convertedRecords = new ArrayList<>(recordFutures.size());
        for (Future<R> f : recordFutures) {
            R record = f.get(); // we need the whole batch, eventually wait forever
            if (record != null) {
                convertedRecords.add(record);
            }
        }

        LOGGER.trace("Calling user handler.");
        userHandler.handleBatch(convertedRecords, committer);
    }
}

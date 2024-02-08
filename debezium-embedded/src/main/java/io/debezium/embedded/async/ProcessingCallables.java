/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.embedded.Transformations;

/**
 * // TODO: Document this
 * @author vjuranek
 */
public class ProcessingCallables {
    /**
     * {@link Callable} which applies transformations to a single record and thereafter passes it to provided consumer.
     */
    public static class TransformAndConsumeRecord implements Callable<Void> {
        private final SourceRecord record;
        private final Transformations transformations;
        private final Consumer<SourceRecord> consumer;

        TransformAndConsumeRecord(final SourceRecord record, final Transformations transformations, final Consumer<SourceRecord> consumer) {
            this.record = record;
            this.transformations = transformations;
            this.consumer = consumer;
        }

        @Override
        public Void call() {
            final SourceRecord transformedRecord = transformations.transform(record);
            if (transformedRecord != null) {
                consumer.accept(transformedRecord);
            }
            return null;
        }
    }

    /**
     * {@link Callable} which applies transformations to a single record.
     */
    public static class TransformRecord implements Callable<SourceRecord> {
        private final SourceRecord record;
        private final Transformations transformations;

        TransformRecord(final SourceRecord record, final Transformations transformations) {
            this.record = record;
            this.transformations = transformations;
        }

        @Override
        public SourceRecord call() {
            final SourceRecord transformedRecord = transformations.transform(record);
            return transformedRecord != null ? transformedRecord : null;
        }
    }

    /**
     * {@link Callable} which applies transformations to a single record and convert the record into desired format.
     */
    public static class TransformAndConvertRecord<R> implements Callable<R> {
        private final SourceRecord record;
        private final Transformations transformations;
        private final Function<SourceRecord, R> converter;

        TransformAndConvertRecord(final SourceRecord record, final Transformations transformations, final Function<SourceRecord, R> converter) {
            this.record = record;
            this.transformations = transformations;
            this.converter = converter;
        }

        @Override
        public R call() {
            final SourceRecord transformedRecord = transformations.transform(record);
            return transformedRecord != null ? converter.apply(transformedRecord) : null;
        }
    }

    /**
     * {@link Callable} which applies transformations to a single record, transformed it into desired format and applies provided use consumer on this record.
     */
    public static class TransformConvertConsumeRecord<R> implements Callable<Void> {
        private final SourceRecord record;
        private final Transformations transformations;
        private final Function<SourceRecord, R> serializer;
        private final Consumer<R> consumer;

        TransformConvertConsumeRecord(final SourceRecord record, final Transformations transformations, final Function<SourceRecord, R> serializer,
                                      final Consumer<R> consumer) {
            this.record = record;
            this.transformations = transformations;
            this.serializer = serializer;
            this.consumer = consumer;
        }

        @Override
        public Void call() {
            final SourceRecord transformedRecord = transformations.transform(record);
            if (transformedRecord != null) {
                consumer.accept(serializer.apply(transformedRecord));
            }
            return null;
        }
    }
}

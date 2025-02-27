/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.embedded.Transformations;

/**
 * Abstract implementation of {@link RecordProcessor}, which provides implementation of processor initialization, while the record processing implementation
 * left to the children classes.
 */
public abstract class AbstractRecordProcessor<R> implements RecordProcessor<R> {
    protected ExecutorService recordService;
    protected Transformations transformations;

    @Override
    public void initialize(final ExecutorService recordService, final Transformations transformations) {
        this.recordService = recordService;
        this.transformations = transformations;
    }

    @Override
    public abstract void processRecords(List<SourceRecord> records) throws Exception;
}

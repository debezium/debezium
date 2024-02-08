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
import io.debezium.engine.DebeziumEngine;

/**
 * Abstract implementation of {@link RecordProcessor}, which provides implementation of processor initialization, while the record processing implementation
 * left to the children classes.
 */
public abstract class AbstractRecordProcessor<R> implements RecordProcessor<R> {
    protected ExecutorService recordService;
    protected Transformations transformations;
    protected DebeziumEngine.RecordCommitter committer;

    @Override
    public void initialize(final ExecutorService recordService, final Transformations transformations, final DebeziumEngine.RecordCommitter committer) {
        this.recordService = recordService;
        this.transformations = transformations;
        this.committer = committer;
    }

    @Override
    public abstract void processRecords(List<SourceRecord> records) throws Exception;
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.common.annotation.Incubating;
import io.debezium.pipeline.signal.channels.process.SignalChannelWriter;

/**
 * {@link DebeziumSourceTask} is a self-contained unit of work created and managed by {@link DebeziumSourceConnector}.
 * In most of the cases the task implementation does actual mining of the changes from the specified resource or its
 * part and further processing of these change records.
 *
 * @author vjuranek
 */
@Incubating
public interface DebeziumSourceTask {
    /**
     * Returns the {@link DebeziumSourceTaskContext} for this DebeziumSourceTask.
     * @return the DebeziumSourceTaskContext for this task
     */
    DebeziumSourceTaskContext context();

    /**
     * Starts the task with the given configuration.
     * @param config task configuration
     */
    void start(Map<String, String> config);

    /**
     * Poll the source for new records.
     * @return list of source records, or null if no records are available
     * @throws InterruptedException if the polling is interrupted
     */
    List<SourceRecord> poll() throws InterruptedException;

    /**
     * Signal the task to stop.
     */
    void stop();

    /**
     * Commit the offsets. Called after a batch of records has been flushed to the offset store.
     * @throws InterruptedException if the commit is interrupted
     */
    void commit() throws InterruptedException;

    /**
     * Commit an individual record.
     * @param record the record to commit
     * @throws InterruptedException if the commit is interrupted
     */
    void commitRecord(SourceRecord record) throws InterruptedException;

    /**
     * Returns the {@link SignalChannelWriter} for this task, if available.
     * @return an optional signal channel writer
     */
    Optional<? extends SignalChannelWriter> signalChannelWriter();
}

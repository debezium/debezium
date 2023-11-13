/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.engine.source;

import java.util.Map;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;

import io.debezium.common.annotation.Incubating;
import io.debezium.embedded.Transformations;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;

/**
 * {@link DebeziumSourceTaskContext} holds useful objects used during the lifecycle of {@link DebeziumSourceTask}.
 *
 * @author vjuranek
 */
@Incubating
public interface DebeziumSourceTaskContext {
    /**
     * Gets the configuration with which the task has been started.
     */
    Map<String, String> config();

    /**
     * Gets the {@link OffsetStorageReader} for this SourceTask.
     */
    OffsetStorageReader offsetStorageReader();

    /**
     * Gets the {@link OffsetStorageWriter} for this SourceTask.
     */
    OffsetStorageWriter offsetStorageWriter();

    /**
     * Gets the {@link OffsetCommitPolicy} for this task.
     */
    OffsetCommitPolicy offsetCommitPolicy();

    /**
     * Gets the {@link Clock} which should be used with {@link OffsetCommitPolicy} for this task.
     */
    Clock clock();

    /**
     * Gets the transformations which the task should apply to source events before passing them to the consumer.
     */
    Transformations transformations();
}

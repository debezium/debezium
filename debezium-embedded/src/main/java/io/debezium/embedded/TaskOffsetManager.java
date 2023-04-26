/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import io.debezium.config.Field;
import io.debezium.engine.spi.OffsetCommitPolicy;

/**
 * Manages an individual task's offsets.
 */
public interface TaskOffsetManager {

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    Field OFFSET_FLUSH_INTERVAL_MS = Field.create("offset.flush.interval.ms")
            .withDescription("Interval at which to try committing offsets, given in milliseconds. Defaults to 1 minute (60,000 ms).")
            .withDefault(60000L)
            .withValidation(Field::isNonNegativeInteger);

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    Field OFFSET_COMMIT_TIMEOUT_MS = Field.create("offset.flush.timeout.ms")
            .withDescription("Time to wait for records to flush and partition offset data to be"
                    + " committed to offset storage before cancelling the process and restoring the offset "
                    + "data to be committed in a future attempt, given in milliseconds. Defaults to 5 seconds (5000 ms).")
            .withDefault(5000L)
            .withValidation(Field::isPositiveInteger);

    Field OFFSET_COMMIT_POLICY = Field.create("offset.commit.policy")
            .withDescription("The fully-qualified class name of the commit policy type. This class must implement the interface "
                    + OffsetCommitPolicy.class.getName()
                    + ". The default is a periodic commit policy based upon time intervals.")
            .withDefault(OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class.getName())
            .withValidation(Field::isClassName);

    /**
     * The array of all exposed fields.
     */
    Field.Set ALL_FIELDS = Field.setOf(OFFSET_FLUSH_INTERVAL_MS, OFFSET_COMMIT_TIMEOUT_MS, OFFSET_FLUSH_INTERVAL_MS);

    /**
     * Possibly commit the offsets.
     */
    void maybeFlush() throws InterruptedException;

    /**
     * Commit the offsets.
     */
    void commitOffsets() throws InterruptedException;

    /**
     * Commit an individual {@link SourceRecord}.
     */
    void commit(SourceRecord record) throws InterruptedException;

    /**
     * Retrieve the {@link OffsetStorageReader} that is managed by this {@link TaskOffsetManager}.
     */
    OffsetStorageReader offsetStorageReader();

}

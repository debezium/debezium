package io.debezium.embedded;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.engine.spi.OffsetCommitPolicy;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;

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

    void maybeFlush() throws InterruptedException;

    void commitOffsets() throws InterruptedException;

    void commit(SourceRecord record) throws InterruptedException;

    OffsetStorageReader offsetStorageReader();

    void configure(Configuration config);

    void stop();
}

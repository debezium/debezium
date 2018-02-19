/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

/**
 * Configuration options common to all Debezium connectors.
 *
 * @author Gunnar Morling
 */
public class CommonConnectorConfig {

    public static final int DEFAULT_MAX_QUEUE_SIZE = 8192;
    public static final int DEFAULT_MAX_BATCH_SIZE = 2048;
    public static final long DEFAULT_POLL_INTERVAL_MILLIS = 500;

    public static final Field TOMBSTONES_ON_DELETE = Field.create("tombstones.on.delete")
            .withDisplayName("Change the behaviour of Debezium with regards to delete operations")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDefault(true)
            .withValidation(Field::isBoolean)
            .withDescription("Whether delete operations should be represented by a delete event and a subsquent" +
                    "tombstone event (true) or only by a delete event (false). Emitting the tombstone event (the" +
                    " default behavior) allows Kafka to completely delete all events pertaining to the given key once" +
                    " the source record got deleted.");

    public static final Field MAX_QUEUE_SIZE = Field.create("max.queue.size")
            .withDisplayName("Change event buffer size")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum size of the queue for change events read from the database log but not yet recorded or forwarded. Defaults to " + DEFAULT_MAX_QUEUE_SIZE + ", and should always be larger than the maximum batch size.")
            .withDefault(DEFAULT_MAX_QUEUE_SIZE)
            .withValidation(CommonConnectorConfig::validateMaxQueueSize);

    public static final Field MAX_BATCH_SIZE = Field.create("max.batch.size")
            .withDisplayName("Change event batch size")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Maximum size of each batch of source records. Defaults to " + DEFAULT_MAX_BATCH_SIZE + ".")
            .withDefault(DEFAULT_MAX_BATCH_SIZE)
            .withValidation(Field::isPositiveInteger);

    public static final Field POLL_INTERVAL_MS = Field.create("poll.interval.ms")
            .withDisplayName("Poll interval (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Frequency in milliseconds to wait for new change events to appear after receiving no events. Defaults to " + DEFAULT_POLL_INTERVAL_MILLIS + "ms.")
            .withDefault(DEFAULT_POLL_INTERVAL_MILLIS)
            .withValidation(Field::isPositiveInteger);

    private final boolean emitTombstoneOnDelete;
    private final int maxQueueSize;
    private final int maxBatchSize;
    private final Duration pollInterval;

    protected CommonConnectorConfig(Configuration config) {
        this.emitTombstoneOnDelete = config.getBoolean(CommonConnectorConfig.TOMBSTONES_ON_DELETE);
        this.maxQueueSize = config.getInteger(MAX_QUEUE_SIZE);
        this.maxBatchSize = config.getInteger(MAX_BATCH_SIZE);
        this.pollInterval = config.getDuration(POLL_INTERVAL_MS, ChronoUnit.MILLIS);
    }

    public boolean isEmitTombstoneOnDelete() {
        return emitTombstoneOnDelete;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    private static int validateMaxQueueSize(Configuration config, Field field, Field.ValidationOutput problems) {
        int maxQueueSize = config.getInteger(field);
        int maxBatchSize = config.getInteger(MAX_BATCH_SIZE);
        int count = 0;
        if (maxQueueSize <= 0) {
            problems.accept(field, maxQueueSize, "A positive queue size is required");
            ++count;
        }
        if (maxQueueSize <= maxBatchSize) {
            problems.accept(field, maxQueueSize, "Must be larger than the maximum batch size");
            ++count;
        }
        return count;
    }
}

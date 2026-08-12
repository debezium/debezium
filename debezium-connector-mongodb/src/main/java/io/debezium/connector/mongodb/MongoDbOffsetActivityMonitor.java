/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.time.Duration;
import java.util.Objects;

import org.bson.BsonTimestamp;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset resume token is compared against the value captured when the monitor was last
 * consulted, and when the token has not moved, a stale result is reported. When the offsets do
 * not yet contain a resume token, i.e. streaming has not observed the first change stream event,
 * the offset timestamp is compared instead.
 * <p>
 * The change stream advances the resume token with every batch, including empty batches, so
 * a stationary token means the connector is not receiving anything from the server rather
 * than that the captured collections are quiet.
 *
 * @author Chris Cranford
 */
public class MongoDbOffsetActivityMonitor implements OffsetActivityMonitor<MongoDbPartition, MongoDbOffsetContext> {

    private final Duration checkInterval;

    private String previousResumeToken;
    private BsonTimestamp previousTimestamp;

    public MongoDbOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(MongoDbPartition partition, MongoDbOffsetContext offsetContext) {
        final String resumeToken = offsetContext.lastResumeToken();
        final BsonTimestamp timestamp = offsetContext.lastTimestamp();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (resumeToken != null) {
            if (Objects.equals(previousResumeToken, resumeToken)) {
                result = StaleOffsetsResult.stale(
                        ("Offset resume token %s has not changed in %d milliseconds. " +
                                "This may indicate the connector is no longer receiving events from the change stream.")
                                .formatted(previousResumeToken, checkInterval.toMillis()));
            }
        }
        else if (previousResumeToken == null && Objects.equals(previousTimestamp, timestamp)) {
            // No resume token has been observed yet; fallback to comparing the offset timestamp
            result = StaleOffsetsResult.stale(
                    ("Offset timestamp %s has not changed in %d milliseconds and no resume token has " +
                            "been received. This may indicate the connector is no longer receiving events from " +
                            "the change stream.")
                            .formatted(previousTimestamp, checkInterval.toMillis()));
        }

        // Update tracked stats
        previousResumeToken = resumeToken;
        previousTimestamp = timestamp;

        return result;
    }

}
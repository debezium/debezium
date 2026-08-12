/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The offset change position, the combination of the commit and change log sequence numbers,
 * is compared per partition against the value captured when the partition was last examined,
 * and when the position has not moved, a stale result is reported. The combination is used
 * rather than the commit log sequence number alone so that progress within a single large
 * transaction is not reported as stale.
 *
 * @author Chris Cranford
 */
public class SqlServerOffsetActivityMonitor implements OffsetActivityMonitor<SqlServerPartition, SqlServerOffsetContext> {

    private final Duration checkInterval;
    private final String taskId;
    private final Map<SqlServerPartition, TxLogPosition> previousPositions = new HashMap<>();

    public SqlServerOffsetActivityMonitor(Duration checkInterval, String taskId) {
        this.checkInterval = checkInterval;
        this.taskId = taskId;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(SqlServerPartition partition, SqlServerOffsetContext offsetContext) {
        final TxLogPosition position = offsetContext.getChangePosition();
        final TxLogPosition previousPosition = previousPositions.get(partition);

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (Objects.equals(previousPosition, position)) {
            result = StaleOffsetsResult.stale(
                    ("Offset position %s for database '%s' on task %s has not changed in at least %d milliseconds. " +
                            "This may indicate the database is idle, there are no changes for the captured tables, " +
                            "or that there are long running transaction(s) delaying the delivery of change events.")
                            .formatted(previousPosition, partition.getDatabaseName(), taskId, checkInterval.toMillis()));
        }

        // Update tracked stats
        previousPositions.put(partition, position);

        return result;
    }

}
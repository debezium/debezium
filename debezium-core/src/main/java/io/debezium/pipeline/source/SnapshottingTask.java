/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source;

import java.util.List;
import java.util.Map;

/**
 * A configuration describing the task to be performed during snapshotting.
 */
public class SnapshottingTask {

    private final boolean snapshotSchema;
    private final boolean snapshotData;
    private final List<String> dataCollections;
    private final Map<String, String> filterQueries;

    private final boolean onDemand;

    public SnapshottingTask(boolean snapshotSchema, boolean snapshotData, List<String> dataCollections, Map<String, String> filterQueries, boolean onDemand) {
        this.snapshotSchema = snapshotSchema;
        this.snapshotData = snapshotData;
        this.dataCollections = dataCollections;
        this.filterQueries = filterQueries;
        this.onDemand = onDemand;
    }

    /**
     * Whether data (rows in captured tables) should be snapshotted.
     */
    public boolean snapshotData() {
        return snapshotData;
    }

    /**
     * Whether the schema of captured tables should be snapshotted.
     */
    public boolean snapshotSchema() {
        return snapshotSchema;
    }

    /**
     * List of regular expression defining the data collection to snapshot
     *
     */
    public List<String> getDataCollections() {
        return dataCollections;
    }

    /**
     * Map of query statement overrides by data collection
     */
    public Map<String, String> getFilterQueries() {
        return filterQueries;
    }

    /**
     * Whether to skip the snapshot phase.
     *
     * By default, this method will skip performing a snapshot if both {@link #snapshotSchema()} and
     * {@link #snapshotData()} return {@code false}.
     */
    public boolean shouldSkipSnapshot() {
        return !snapshotSchema() && !snapshotData();
    }

    /**
     * Determine if the task is an onDemand snapshot or not
     *
     */
    public boolean isOnDemand() {
        return onDemand;
    }

    @Override
    public String toString() {
        return "SnapshottingTask [snapshotSchema=" + snapshotSchema + ", snapshotData=" + snapshotData + "]";
    }
}

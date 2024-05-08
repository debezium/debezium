/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.meters.SnapshotMeter;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;

class SqlServerSnapshotPartitionMetrics extends AbstractSqlServerPartitionMetrics
        implements SqlServerSnapshotPartitionMetricsMXBean {

    private final SnapshotMeter snapshotMeter;

    SqlServerSnapshotPartitionMetrics(CdcSourceTaskContext taskContext, Map<String, String> tags,
                                      EventMetadataProvider metadataProvider) {
        super(taskContext, tags, metadataProvider);
        snapshotMeter = new SnapshotMeter(taskContext.getClock());
    }

    @Override
    public int getTotalTableCount() {
        return snapshotMeter.getTotalTableCount();
    }

    @Override
    public int getRemainingTableCount() {
        return snapshotMeter.getRemainingTableCount();
    }

    @Override
    public boolean getSnapshotRunning() {
        return snapshotMeter.getSnapshotRunning();
    }

    @Override
    public boolean getSnapshotPaused() {
        return snapshotMeter.getSnapshotPaused();
    }

    @Override
    public boolean getSnapshotCompleted() {
        return snapshotMeter.getSnapshotCompleted();
    }

    @Override
    public boolean getSnapshotAborted() {
        return snapshotMeter.getSnapshotAborted();
    }

    @Override
    public long getSnapshotDurationInSeconds() {
        return snapshotMeter.getSnapshotDurationInSeconds();
    }

    @Override
    public long getSnapshotPausedDurationInSeconds() {
        return snapshotMeter.getSnapshotPausedDurationInSeconds();
    }

    @Override
    public String[] getCapturedTables() {
        return snapshotMeter.getCapturedTables();
    }

    void monitoredDataCollectionsDetermined(Iterable<? extends DataCollectionId> dataCollectionIds) {
        snapshotMeter.monitoredDataCollectionsDetermined(dataCollectionIds);
    }

    void dataCollectionSnapshotCompleted(DataCollectionId dataCollectionId, long numRows) {
        snapshotMeter.dataCollectionSnapshotCompleted(dataCollectionId, numRows);
    }

    void snapshotStarted() {
        snapshotMeter.snapshotStarted();
    }

    void snapshotPaused() {
        snapshotMeter.snapshotPaused();
    }

    void snapshotResumed() {
        snapshotMeter.snapshotResumed();
    }

    void snapshotCompleted() {
        snapshotMeter.snapshotCompleted();
    }

    void snapshotAborted() {
        snapshotMeter.snapshotAborted();
    }

    void rowsScanned(TableId tableId, long numRows) {
        snapshotMeter.rowsScanned(tableId, numRows);
    }

    @Override
    public ConcurrentMap<String, Long> getRowsScanned() {
        return snapshotMeter.getRowsScanned();
    }

    void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        snapshotMeter.currentChunk(chunkId, chunkFrom, chunkTo);
    }

    void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo, Object tableTo[]) {
        snapshotMeter.currentChunk(chunkId, chunkFrom, chunkTo, tableTo);
    }

    @Override
    public String getChunkId() {
        return snapshotMeter.getChunkId();
    }

    @Override
    public String getChunkFrom() {
        return snapshotMeter.getChunkFrom();
    }

    @Override
    public String getChunkTo() {
        return snapshotMeter.getChunkTo();
    }

    @Override
    public String getTableFrom() {
        return snapshotMeter.getTableFrom();
    }

    @Override
    public String getTableTo() {
        return snapshotMeter.getTableTo();
    }

    @Override
    public void reset() {
        snapshotMeter.reset();
    }
}

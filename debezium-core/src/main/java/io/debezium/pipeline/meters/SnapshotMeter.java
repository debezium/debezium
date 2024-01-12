/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.meters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.metrics.traits.SnapshotMetricsMXBean;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Carries snapshot metrics.
 */
@ThreadSafe
public class SnapshotMeter implements SnapshotMetricsMXBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotMeter.class);

    private final AtomicBoolean snapshotRunning = new AtomicBoolean();
    private final AtomicBoolean snapshotPaused = new AtomicBoolean();
    private final AtomicBoolean snapshotCompleted = new AtomicBoolean();
    private final AtomicBoolean snapshotAborted = new AtomicBoolean();
    private final AtomicLong startTime = new AtomicLong();
    private final AtomicLong stopTime = new AtomicLong();
    private final AtomicLong startPauseTime = new AtomicLong();
    private final AtomicLong stopPauseTime = new AtomicLong();
    private final AtomicLong pauseDuration = new AtomicLong();
    private final ConcurrentMap<String, Long> rowsScanned = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, String> remainingTables = new ConcurrentHashMap<>();

    private final AtomicReference<String> chunkId = new AtomicReference<>();
    private final AtomicReference<Object[]> chunkFrom = new AtomicReference<>();
    private final AtomicReference<Object[]> chunkTo = new AtomicReference<>();
    private final AtomicReference<Object[]> tableFrom = new AtomicReference<>();
    private final AtomicReference<Object[]> tableTo = new AtomicReference<>();

    private final Set<String> capturedTables = Collections.synchronizedSet(new HashSet<>());

    private final Clock clock;

    public SnapshotMeter(Clock clock) {
        this.clock = clock;
    }

    @Override
    public int getTotalTableCount() {
        return this.capturedTables.size();
    }

    @Override
    public int getRemainingTableCount() {
        return this.remainingTables.size();
    }

    @Override
    public boolean getSnapshotRunning() {
        return this.snapshotRunning.get();
    }

    @Override
    public boolean getSnapshotPaused() {
        return this.snapshotPaused.get();
    }

    @Override
    public boolean getSnapshotCompleted() {
        return this.snapshotCompleted.get();
    }

    @Override
    public boolean getSnapshotAborted() {
        return this.snapshotAborted.get();
    }

    @Override
    public long getSnapshotDurationInSeconds() {
        final long startMillis = startTime.get();
        if (startMillis <= 0L) {
            return 0;
        }
        long stopMillis = stopTime.get();
        if (stopMillis == 0L) {
            stopMillis = clock.currentTimeInMillis();
        }
        return (stopMillis - startMillis) / 1000L;
    }

    @Override
    public long getSnapshotPausedDurationInSeconds() {
        final long pausedMillis = pauseDuration.get();
        return pausedMillis < 0 ? 0 : pausedMillis / 1000L;
    }

    @Override
    public String[] getCapturedTables() {
        return capturedTables.toArray(new String[0]);
    }

    public void monitoredDataCollectionsDetermined(Iterable<? extends DataCollectionId> dataCollectionIds) {
        for (DataCollectionId dataCollectionId : dataCollectionIds) {
            this.remainingTables.put(dataCollectionId.toDoubleQuotedString(), "");
            capturedTables.add(dataCollectionId.toDoubleQuotedString());
        }
    }

    public void dataCollectionSnapshotCompleted(DataCollectionId dataCollectionId, long numRows) {
        rowsScanned.put(dataCollectionId.toDoubleQuotedString(), numRows);
        remainingTables.remove(dataCollectionId.toDoubleQuotedString());
    }

    public void snapshotStarted() {
        this.snapshotRunning.set(true);
        this.snapshotPaused.set(false);
        this.snapshotCompleted.set(false);
        this.snapshotAborted.set(false);
        this.startTime.set(clock.currentTimeInMillis());
        this.stopTime.set(0L);
        this.startPauseTime.set(0);
        this.stopPauseTime.set(0);
        this.pauseDuration.set(0);
    }

    public void snapshotPaused() {
        this.snapshotRunning.set(false);
        this.snapshotPaused.set(true);
        this.snapshotCompleted.set(false);
        this.snapshotAborted.set(false);
        this.startPauseTime.set(clock.currentTimeInMillis());
        this.stopPauseTime.set(0L);
    }

    public void snapshotResumed() {
        this.snapshotRunning.set(true);
        this.snapshotPaused.set(false);
        this.snapshotCompleted.set(false);
        this.snapshotAborted.set(false);
        final long currTime = clock.currentTimeInMillis();
        this.stopPauseTime.set(currTime);

        long pauseStartTime = this.startPauseTime.get();
        if (pauseStartTime < 0L) {
            pauseStartTime = currTime;
        }
        long pausedTime = this.pauseDuration.get();
        if (pausedTime < 0L) {
            pausedTime = 0;
        }

        this.pauseDuration.set(pausedTime + currTime - pauseStartTime);
    }

    public void snapshotCompleted() {
        this.snapshotCompleted.set(true);
        this.snapshotAborted.set(false);
        this.snapshotRunning.set(false);
        this.snapshotPaused.set(false);
        this.stopTime.set(clock.currentTimeInMillis());
    }

    public void snapshotAborted() {
        this.snapshotCompleted.set(false);
        this.snapshotAborted.set(true);
        this.snapshotRunning.set(false);
        this.snapshotPaused.set(false);
        this.stopTime.set(clock.currentTimeInMillis());
    }

    public void rowsScanned(TableId tableId, long numRows) {
        rowsScanned.put(tableId.toDoubleQuotedString(), numRows);
    }

    @Override
    public ConcurrentMap<String, Long> getRowsScanned() {
        return rowsScanned;
    }

    public void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        this.chunkId.set(chunkId);
        this.chunkFrom.set(chunkFrom);
        this.chunkTo.set(chunkTo);
    }

    public void currentChunk(String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        currentChunk(chunkId, chunkFrom, chunkTo);
        this.tableFrom.set(chunkFrom);
        this.tableTo.set(tableTo);
    }

    @Override
    public String getChunkId() {
        return chunkId.get();
    }

    @Override
    public String getChunkFrom() {
        return arrayToString(chunkFrom.get());
    }

    @Override
    public String getChunkTo() {
        return arrayToString(chunkTo.get());
    }

    @Override
    public String getTableFrom() {
        return arrayToString(tableFrom.get());
    }

    @Override
    public String getTableTo() {
        return arrayToString(tableTo.get());
    }

    private String arrayToString(Object[] array) {
        return (array == null) ? null : Arrays.toString(array);
    }

    public void reset() {
        snapshotRunning.set(false);
        snapshotPaused.set(false);
        snapshotCompleted.set(false);
        snapshotAborted.set(false);
        startTime.set(0);
        stopTime.set(0);
        startPauseTime.set(0);
        stopPauseTime.set(0);
        pauseDuration.set(0);
        rowsScanned.clear();
        remainingTables.clear();
        capturedTables.clear();
        chunkId.set(null);
        chunkFrom.set(null);
        chunkTo.set(null);
        tableFrom.set(null);
        tableTo.set(null);
    }
}

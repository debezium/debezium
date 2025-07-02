/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;

/**
 * A thread-safe state manager for incremental snapshots that centralizes the management
 * of shared state to prevent race conditions and provide consistent operations.
 *
 * This class manages:
 * - Signal-based data collection operations (add/stop requests)
 * - Snapshot state control (pause/resume)
 * - Window management for deduplication
 * - Thread-safe access to shared resources
 *
 * @author Debezium Authors
 */
@ThreadSafe
public class IncrementalSnapshotStateManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncrementalSnapshotStateManager.class);

    // Signal-based operations
    private final ConcurrentLinkedQueue<SignalDataCollection> dataCollectionsToAdd = new ConcurrentLinkedQueue<>();
    private final LinkedBlockingQueue<String> dataCollectionsToStop = new LinkedBlockingQueue<>();

    // Snapshot state control
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private volatile boolean windowOpened = false;
    private volatile String currentChunkId;

    // Thread-safe synchronization for window operations
    private final ReadWriteLock windowLock = new ReentrantReadWriteLock();

    /**
     * Requests to add data collections to the snapshot.
     * This operation is thread-safe and can be called concurrently.
     *
     * @param signalPayload the signal payload containing collection information
     * @param snapshotConfiguration the snapshot configuration
     */
    public void requestAddDataCollectionToSnapshot(SignalPayload signalPayload, SnapshotConfiguration snapshotConfiguration) {
        SignalDataCollection dataCollection = new SignalDataCollection(signalPayload, snapshotConfiguration);
        dataCollectionsToAdd.add(dataCollection);
        LOGGER.debug("Added data collection to snapshot queue: {}", signalPayload.id);
    }

    /**
     * Polls and returns the next data collection to be added to the snapshot.
     * This operation is thread-safe and removes the element from the queue atomically.
     *
     * @return the next SignalDataCollection to add, or null if queue is empty
     */
    public SignalDataCollection pollDataCollectionToAdd() {
        return dataCollectionsToAdd.poll();
    }

    /**
     * Requests to stop snapshot processing for the specified data collections.
     * This operation is thread-safe and can be called concurrently.
     *
     * @param dataCollectionIds list of data collection identifiers to stop, or null/empty for all
     */
    public void requestStopSnapshot(List<String> dataCollectionIds) {
        if (dataCollectionIds == null || dataCollectionIds.isEmpty()) {
            dataCollectionsToStop.add(".*");
            LOGGER.debug("Requested to stop all data collections");
        }
        else {
            dataCollectionsToStop.addAll(dataCollectionIds);
            LOGGER.debug("Requested to stop data collections: {}", dataCollectionIds);
        }
    }

    /**
     * Drains all pending stop requests into the provided list.
     * This operation is thread-safe and atomic.
     *
     * @return list of data collection identifiers that should be stopped
     */
    public List<String> drainDataCollectionsToStop() {
        List<String> drainedList = new ArrayList<>();
        dataCollectionsToStop.drainTo(drainedList);
        return drainedList;
    }

    /**
     * Pauses the incremental snapshot.
     * This operation is thread-safe and idempotent.
     */
    public void pauseSnapshot() {
        if (paused.compareAndSet(false, true)) {
            LOGGER.info("Incremental snapshot paused");
        }
        else {
            LOGGER.debug("Incremental snapshot is already paused");
        }
    }

    /**
     * Resumes the incremental snapshot.
     * This operation is thread-safe and idempotent.
     */
    public void resumeSnapshot() {
        if (paused.compareAndSet(true, false)) {
            LOGGER.info("Incremental snapshot resumed");
        }
        else {
            LOGGER.debug("Incremental snapshot is not paused");
        }
    }

    /**
     * Checks if the incremental snapshot is currently paused.
     *
     * @return true if paused, false otherwise
     */
    public boolean isSnapshotPaused() {
        return paused.get();
    }

    /**
     * Opens the snapshot window for the specified chunk ID.
     * This operation is thread-safe and validates the chunk ID.
     *
     * @param chunkId the chunk ID to open the window for
     * @return true if window was opened, false if request was ignored
     */
    public boolean openWindow(String chunkId) {
        windowLock.writeLock().lock();
        try {
            if (isUnexpectedChunk(chunkId)) {
                LOGGER.info("Received request to open window with id = '{}', expected = '{}', request ignored",
                        chunkId, currentChunkId);
                return false;
            }
            LOGGER.debug("Opening window for incremental snapshot chunk: {}", chunkId);
            windowOpened = true;
            return true;
        }
        finally {
            windowLock.writeLock().unlock();
        }
    }

    /**
     * Closes the snapshot window for the specified chunk ID.
     * This operation is thread-safe and validates the chunk ID.
     *
     * @param chunkId the chunk ID to close the window for
     * @return true if window was closed, false if request was ignored
     */
    public boolean closeWindow(String chunkId) {
        windowLock.writeLock().lock();
        try {
            if (isUnexpectedChunk(chunkId)) {
                LOGGER.info("Received request to close window with id = '{}', expected = '{}', request ignored",
                        chunkId, currentChunkId);
                return false;
            }
            LOGGER.debug("Closing window for incremental snapshot chunk: {}", chunkId);
            windowOpened = false;
            return true;
        }
        finally {
            windowLock.writeLock().unlock();
        }
    }

    /**
     * Checks if deduplication is needed based on window state.
     * This operation is thread-safe for reading.
     *
     * @return true if deduplication is needed (window is open), false otherwise
     */
    public boolean deduplicationNeeded() {
        windowLock.readLock().lock();
        try {
            return windowOpened;
        }
        finally {
            windowLock.readLock().unlock();
        }
    }

    /**
     * Sets the current chunk ID for window validation.
     * This operation is thread-safe.
     *
     * @param chunkId the current chunk ID
     */
    public void setCurrentChunkId(String chunkId) {
        windowLock.writeLock().lock();
        try {
            this.currentChunkId = chunkId;
            LOGGER.debug("Set current chunk ID to: {}", chunkId);
        }
        finally {
            windowLock.writeLock().unlock();
        }
    }

    /**
     * Gets the current chunk ID.
     * This operation is thread-safe for reading.
     *
     * @return the current chunk ID
     */
    public String getCurrentChunkId() {
        windowLock.readLock().lock();
        try {
            return currentChunkId;
        }
        finally {
            windowLock.readLock().unlock();
        }
    }

    /**
     * Checks if the given chunk ID is unexpected (doesn't match current chunk).
     * This method should be called within appropriate locks.
     *
     * @param chunkId the chunk ID to validate
     * @return true if the chunk ID is unexpected, false otherwise
     */
    private boolean isUnexpectedChunk(String chunkId) {
        return currentChunkId == null || !chunkId.startsWith(currentChunkId);
    }

    /**
     * Clears all pending operations and resets state.
     * This operation is thread-safe and can be used for cleanup.
     */
    public void clear() {
        windowLock.writeLock().lock();
        try {
            dataCollectionsToAdd.clear();
            dataCollectionsToStop.clear();
            paused.set(false);
            windowOpened = false;
            currentChunkId = null;
            LOGGER.debug("Cleared incremental snapshot state manager");
        }
        finally {
            windowLock.writeLock().unlock();
        }
    }

    /**
     * Gets the number of pending data collections to be added.
     *
     * @return the size of the add queue
     */
    public int getPendingAddCount() {
        return dataCollectionsToAdd.size();
    }

    /**
     * Gets the number of pending data collections to be stopped.
     *
     * @return the size of the stop queue
     */
    public int getPendingStopCount() {
        return dataCollectionsToStop.size();
    }

    /**
     * Returns a summary of the current state for debugging purposes.
     *
     * @return string representation of current state
     */
    @Override
    public String toString() {
        windowLock.readLock().lock();
        try {
            return String.format("IncrementalSnapshotStateManager{" +
                    "pendingAdds=%d, pendingStops=%d, paused=%s, windowOpened=%s, currentChunkId='%s'}",
                    getPendingAddCount(), getPendingStopCount(),
                    paused.get(), windowOpened, currentChunkId);
        }
        finally {
            windowLock.readLock().unlock();
        }
    }
}
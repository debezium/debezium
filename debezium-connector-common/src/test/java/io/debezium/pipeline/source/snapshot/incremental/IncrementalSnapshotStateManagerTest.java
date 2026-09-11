/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;

/**
 * Test for IncrementalSnapshotStateManager to verify thread-safe operations
 * and race condition prevention.
 */
public class IncrementalSnapshotStateManagerTest {

    @Test
    public void testPauseResumeOperations() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();

        // Initial state should not be paused
        assertFalse(stateManager.isSnapshotPaused(), "Should not be paused initially");

        // Pause operation
        stateManager.pauseSnapshot();
        assertTrue(stateManager.isSnapshotPaused(), "Should be paused after pause");

        // Idempotent pause
        stateManager.pauseSnapshot();
        assertTrue(stateManager.isSnapshotPaused(), "Should remain paused after second pause");

        // Resume operation
        stateManager.resumeSnapshot();
        assertFalse(stateManager.isSnapshotPaused(), "Should not be paused after resume");

        // Idempotent resume
        stateManager.resumeSnapshot();
        assertFalse(stateManager.isSnapshotPaused(), "Should remain not paused after second resume");
    }

    @Test
    public void testWindowOperations() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();
        String chunkId = "test-chunk-123";

        // Set current chunk ID
        stateManager.setCurrentChunkId(chunkId);
        assertEquals(chunkId, stateManager.getCurrentChunkId(), "Chunk ID should match");

        // Open window
        assertTrue(stateManager.openWindow(chunkId), "Should open window for valid chunk");
        assertTrue(stateManager.deduplicationNeeded(), "Should need deduplication when window is open");

        // Try to open with different chunk (should be ignored)
        assertFalse(stateManager.openWindow("different-chunk"), "Should ignore window open for different chunk");

        // Close window
        assertTrue(stateManager.closeWindow(chunkId), "Should close window for valid chunk");
        assertFalse(stateManager.deduplicationNeeded(), "Should not need deduplication when window is closed");

        // Try to close with different chunk (should be ignored)
        assertFalse(stateManager.closeWindow("different-chunk"), "Should ignore window close for different chunk");
    }

    @Test
    public void testDataCollectionOperations() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();

        // Test add operations
        SignalPayload payload1 = createMockSignalPayload("collection1");
        SignalPayload payload2 = createMockSignalPayload("collection2");
        SnapshotConfiguration config = new SnapshotConfiguration();

        stateManager.requestAddDataCollectionToSnapshot(payload1, config);
        stateManager.requestAddDataCollectionToSnapshot(payload2, config);

        assertEquals(2, stateManager.getPendingAddCount(), "Should have 2 pending adds");

        // Test polling
        SignalDataCollection polled1 = stateManager.pollDataCollectionToAdd();
        assertNotNull(polled1, "Should poll first collection");
        assertEquals(payload1, polled1.getSignalPayload(), "Should poll correct collection");

        SignalDataCollection polled2 = stateManager.pollDataCollectionToAdd();
        assertNotNull(polled2, "Should poll second collection");
        assertEquals(payload2, polled2.getSignalPayload(), "Should poll correct collection");

        // No more collections
        assertNull(stateManager.pollDataCollectionToAdd(), "Should return null when no more collections");
        assertEquals(0, stateManager.getPendingAddCount(), "Should have 0 pending adds");

        // Test stop operations
        List<String> collectionsToStop = Arrays.asList("table1", "table2");
        stateManager.requestStopSnapshot(collectionsToStop);

        assertEquals(2, stateManager.getPendingStopCount(), "Should have 2 pending stops");

        List<String> stoppedCollections = stateManager.drainDataCollectionsToStop();
        assertEquals(2, stoppedCollections.size(), "Should drain all collections");
        assertTrue(stoppedCollections.contains("table1"), "Should contain table1");
        assertTrue(stoppedCollections.contains("table2"), "Should contain table2");

        assertEquals(0, stateManager.getPendingStopCount(), "Should have 0 pending stops after drain");
    }

    @Test
    public void testConcurrentOperations() throws InterruptedException {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();
        int numThreads = 10;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger addedCount = new AtomicInteger(0);

        // Start multiple threads that add collections concurrently
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        SignalPayload payload = createMockSignalPayload("collection_" + threadId + "_" + j);
                        SnapshotConfiguration config = new SnapshotConfiguration();
                        stateManager.requestAddDataCollectionToSnapshot(payload, config);
                        addedCount.incrementAndGet();
                    }
                }
                finally {
                    latch.countDown();
                }
            });
        }

        latch.await(); // Wait for all threads to complete
        executor.shutdown();

        // Verify all operations completed without race conditions
        assertEquals(numThreads * operationsPerThread, addedCount.get(), "All collections should be added");
        assertEquals(numThreads * operationsPerThread, stateManager.getPendingAddCount(),
                "State manager should have all pending collections");

        // Poll all collections to verify queue integrity
        int polledCount = 0;
        while (stateManager.pollDataCollectionToAdd() != null) {
            polledCount++;
        }

        assertEquals(numThreads * operationsPerThread, polledCount, "Should poll all added collections");
    }

    @Test
    public void testStopAllCollections() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();

        // Request stop with null should stop all
        stateManager.requestStopSnapshot(null);

        List<String> stopped = stateManager.drainDataCollectionsToStop();
        assertEquals(1, stopped.size(), "Should have wildcard stop");
        assertEquals(".*", stopped.get(0), "Should contain wildcard");

        // Request stop with empty list should also stop all
        stateManager.requestStopSnapshot(Arrays.asList());

        stopped = stateManager.drainDataCollectionsToStop();
        assertEquals(1, stopped.size(), "Should have wildcard stop");
        assertEquals(".*", stopped.get(0), "Should contain wildcard");
    }

    @Test
    public void testClearOperations() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();

        // Set up some state
        stateManager.setCurrentChunkId("test-chunk");
        stateManager.pauseSnapshot();
        stateManager.openWindow("test-chunk");
        stateManager.requestAddDataCollectionToSnapshot(createMockSignalPayload("test"), new SnapshotConfiguration());
        stateManager.requestStopSnapshot(Arrays.asList("table1"));

        // Verify state is set
        assertTrue(stateManager.isSnapshotPaused());
        assertTrue(stateManager.deduplicationNeeded());
        assertEquals(1, stateManager.getPendingAddCount());
        assertEquals(1, stateManager.getPendingStopCount());

        // Clear all state
        stateManager.clear();

        // Verify everything is cleared
        assertFalse(stateManager.isSnapshotPaused(), "Should not be paused after clear");
        assertFalse(stateManager.deduplicationNeeded(), "Should not need deduplication after clear");
        assertEquals(0, stateManager.getPendingAddCount(), "Should have no pending adds");
        assertEquals(0, stateManager.getPendingStopCount(), "Should have no pending stops");
        assertNull(stateManager.getCurrentChunkId(), "Current chunk ID should be null");
    }

    @Test
    public void testToString() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();

        String initialState = stateManager.toString();
        assertTrue(initialState.contains("IncrementalSnapshotStateManager"), "Should contain class name");
        assertTrue(initialState.contains("pendingAdds=0"), "Should contain state info");
        assertTrue(initialState.contains("paused=false"), "Should contain pause state");

        // Add some state and verify toString reflects changes
        stateManager.pauseSnapshot();
        stateManager.setCurrentChunkId("test");
        stateManager.requestAddDataCollectionToSnapshot(createMockSignalPayload("test"), new SnapshotConfiguration());

        String stateWithData = stateManager.toString();
        assertTrue(stateWithData.contains("paused=true"), "Should show paused state");
        assertTrue(stateWithData.contains("pendingAdds=1"), "Should show pending adds");
        assertTrue(stateWithData.contains("currentChunkId='test'"), "Should show chunk ID");
    }

    private SignalPayload createMockSignalPayload(String id) {
        // Create a simple mock SignalPayload for testing
        return new SignalPayload(null, id, "test-type", null, null, null);
    }
}
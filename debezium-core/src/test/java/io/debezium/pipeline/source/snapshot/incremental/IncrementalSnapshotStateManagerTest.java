/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

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
        assertFalse("Should not be paused initially", stateManager.isSnapshotPaused());

        // Pause operation
        stateManager.pauseSnapshot();
        assertTrue("Should be paused after pause", stateManager.isSnapshotPaused());

        // Idempotent pause
        stateManager.pauseSnapshot();
        assertTrue("Should remain paused after second pause", stateManager.isSnapshotPaused());

        // Resume operation
        stateManager.resumeSnapshot();
        assertFalse("Should not be paused after resume", stateManager.isSnapshotPaused());

        // Idempotent resume
        stateManager.resumeSnapshot();
        assertFalse("Should remain not paused after second resume", stateManager.isSnapshotPaused());
    }

    @Test
    public void testWindowOperations() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();
        String chunkId = "test-chunk-123";

        // Set current chunk ID
        stateManager.setCurrentChunkId(chunkId);
        assertEquals("Chunk ID should match", chunkId, stateManager.getCurrentChunkId());

        // Open window
        assertTrue("Should open window for valid chunk", stateManager.openWindow(chunkId));
        assertTrue("Should need deduplication when window is open", stateManager.deduplicationNeeded());

        // Try to open with different chunk (should be ignored)
        assertFalse("Should ignore window open for different chunk", stateManager.openWindow("different-chunk"));

        // Close window
        assertTrue("Should close window for valid chunk", stateManager.closeWindow(chunkId));
        assertFalse("Should not need deduplication when window is closed", stateManager.deduplicationNeeded());

        // Try to close with different chunk (should be ignored)
        assertFalse("Should ignore window close for different chunk", stateManager.closeWindow("different-chunk"));
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

        assertEquals("Should have 2 pending adds", 2, stateManager.getPendingAddCount());

        // Test polling
        SignalDataCollection polled1 = stateManager.pollDataCollectionToAdd();
        assertNotNull("Should poll first collection", polled1);
        assertEquals("Should poll correct collection", payload1, polled1.getSignalPayload());

        SignalDataCollection polled2 = stateManager.pollDataCollectionToAdd();
        assertNotNull("Should poll second collection", polled2);
        assertEquals("Should poll correct collection", payload2, polled2.getSignalPayload());

        // No more collections
        assertNull("Should return null when no more collections", stateManager.pollDataCollectionToAdd());
        assertEquals("Should have 0 pending adds", 0, stateManager.getPendingAddCount());

        // Test stop operations
        List<String> collectionsToStop = Arrays.asList("table1", "table2");
        stateManager.requestStopSnapshot(collectionsToStop);

        assertEquals("Should have 2 pending stops", 2, stateManager.getPendingStopCount());

        List<String> stoppedCollections = stateManager.drainDataCollectionsToStop();
        assertEquals("Should drain all collections", 2, stoppedCollections.size());
        assertTrue("Should contain table1", stoppedCollections.contains("table1"));
        assertTrue("Should contain table2", stoppedCollections.contains("table2"));

        assertEquals("Should have 0 pending stops after drain", 0, stateManager.getPendingStopCount());
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
        assertEquals("All collections should be added", numThreads * operationsPerThread, addedCount.get());
        assertEquals("State manager should have all pending collections",
                numThreads * operationsPerThread, stateManager.getPendingAddCount());

        // Poll all collections to verify queue integrity
        int polledCount = 0;
        while (stateManager.pollDataCollectionToAdd() != null) {
            polledCount++;
        }

        assertEquals("Should poll all added collections", numThreads * operationsPerThread, polledCount);
    }

    @Test
    public void testStopAllCollections() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();

        // Request stop with null should stop all
        stateManager.requestStopSnapshot(null);

        List<String> stopped = stateManager.drainDataCollectionsToStop();
        assertEquals("Should have wildcard stop", 1, stopped.size());
        assertEquals("Should contain wildcard", ".*", stopped.get(0));

        // Request stop with empty list should also stop all
        stateManager.requestStopSnapshot(Arrays.asList());

        stopped = stateManager.drainDataCollectionsToStop();
        assertEquals("Should have wildcard stop", 1, stopped.size());
        assertEquals("Should contain wildcard", ".*", stopped.get(0));
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
        assertFalse("Should not be paused after clear", stateManager.isSnapshotPaused());
        assertFalse("Should not need deduplication after clear", stateManager.deduplicationNeeded());
        assertEquals("Should have no pending adds", 0, stateManager.getPendingAddCount());
        assertEquals("Should have no pending stops", 0, stateManager.getPendingStopCount());
        assertNull("Current chunk ID should be null", stateManager.getCurrentChunkId());
    }

    @Test
    public void testToString() {
        IncrementalSnapshotStateManager stateManager = new IncrementalSnapshotStateManager();

        String initialState = stateManager.toString();
        assertTrue("Should contain class name", initialState.contains("IncrementalSnapshotStateManager"));
        assertTrue("Should contain state info", initialState.contains("pendingAdds=0"));
        assertTrue("Should contain pause state", initialState.contains("paused=false"));

        // Add some state and verify toString reflects changes
        stateManager.pauseSnapshot();
        stateManager.setCurrentChunkId("test");
        stateManager.requestAddDataCollectionToSnapshot(createMockSignalPayload("test"), new SnapshotConfiguration());

        String stateWithData = stateManager.toString();
        assertTrue("Should show paused state", stateWithData.contains("paused=true"));
        assertTrue("Should show pending adds", stateWithData.contains("pendingAdds=1"));
        assertTrue("Should show chunk ID", stateWithData.contains("currentChunkId='test'"));
    }

    private SignalPayload createMockSignalPayload(String id) {
        // Create a simple mock SignalPayload for testing
        return new SignalPayload(null, id, "test-type", null, null, null);
    }
}
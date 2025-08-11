/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.base;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.pipeline.DataChangeEvent;
import io.debezium.util.LoggingContext;

/**
 * Test for the queue shutdown mechanism that prevents coordinator thread memory leaks.
 *
 * This test validates the fix for the issue where coordinator threads get stuck
 * in infinite blocking loops when the queue is full and the consumer thread dies.
 *
 * @author Yashi Srivastava
 */
public class ChangeEventQueueShutdownTest {

    /**
     * Test that shutdown mechanism prevents infinite blocking in doEnqueue
     */
    @Test
    public void shouldUnblockThreadsOnShutdown() throws Exception {
        // Create a small queue to make it easy to fill
        int queueSize = 5;
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(3)
                .maxQueueSize(queueSize)
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
                .pollInterval(Duration.ofMillis(100))
                .build();

        // Fill the queue to capacity
        DataChangeEvent testEvent = createTestEvent();
        for (int i = 0; i < queueSize; i++) {
            queue.enqueue(testEvent);
        }

        // Create thread that will block trying to enqueue
        AtomicBoolean enqueueCompleted = new AtomicBoolean(false);
        AtomicReference<Exception> enqueueException = new AtomicReference<>();
        CountDownLatch enqueueStarted = new CountDownLatch(1);
        CountDownLatch enqueueFinished = new CountDownLatch(1);

        Thread enqueueThread = new Thread(() -> {
            try {
                enqueueStarted.countDown();
                // This should block because queue is full
                queue.enqueue(testEvent);
                enqueueCompleted.set(true);
            }
            catch (InterruptedException e) {
                enqueueException.set(e);
            }
            catch (Exception e) {
                enqueueException.set(e);
            }
            finally {
                enqueueFinished.countDown();
            }
        });

        enqueueThread.start();

        // Wait for thread to start and get blocked
        assertTrue("Enqueue thread should start", enqueueStarted.await(5, TimeUnit.SECONDS));
        Thread.sleep(200); // Give time to reach blocking condition

        // Verify thread is alive and blocked
        assertTrue("Thread should be alive and blocked", enqueueThread.isAlive());

        // Shutdown queue - should unblock the thread
        long shutdownStart = System.currentTimeMillis();
        queue.shutdown();

        // Wait for thread to finish
        boolean threadFinished = enqueueFinished.await(5, TimeUnit.SECONDS);
        long totalDuration = System.currentTimeMillis() - shutdownStart;

        // Verify shutdown worked
        assertTrue("Thread should finish after shutdown", threadFinished);
        assertTrue("Shutdown should be quick (under 2 seconds)", totalDuration < 2000);

        // Verify the enqueue was interrupted
        assertFalse("Enqueue should not complete after shutdown", enqueueCompleted.get());
        assertTrue("Should have an exception", enqueueException.get() != null);
        assertTrue("Should be InterruptedException", enqueueException.get() instanceof InterruptedException);
        assertTrue("Should mention shutdown", enqueueException.get().getMessage().contains("shut down"));
    }

    /**
     * Test multiple threads are unblocked by shutdown
     */
    @Test
    public void shouldUnblockMultipleThreads() throws Exception {
        int queueSize = 3;
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(2)
                .maxQueueSize(queueSize)
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
                .pollInterval(Duration.ofMillis(50))
                .build();

        // Fill queue to capacity
        DataChangeEvent testEvent = createTestEvent();
        for (int i = 0; i < queueSize; i++) {
            queue.enqueue(testEvent);
        }

        // Create multiple blocked threads
        int numThreads = 3;
        Thread[] threads = new Thread[numThreads];
        CountDownLatch allStarted = new CountDownLatch(numThreads);
        CountDownLatch allFinished = new CountDownLatch(numThreads);
        AtomicReference<Exception>[] exceptions = new AtomicReference[numThreads];

        for (int i = 0; i < numThreads; i++) {
            exceptions[i] = new AtomicReference<>();
            final int threadIndex = i;

            threads[i] = new Thread(() -> {
                try {
                    allStarted.countDown();
                    queue.enqueue(testEvent);
                }
                catch (Exception e) {
                    exceptions[threadIndex].set(e);
                }
                finally {
                    allFinished.countDown();
                }
            });

            threads[i].start();
        }

        // Wait for all threads to start and get blocked
        assertTrue("All threads should start", allStarted.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);

        // Verify all threads are alive
        for (Thread thread : threads) {
            assertTrue("Thread should be alive", thread.isAlive());
        }

        // Shutdown should unblock all threads
        queue.shutdown();
        boolean threadsFinished = allFinished.await(5, TimeUnit.SECONDS);

        assertTrue("All threads should finish after shutdown", threadsFinished);

        // Verify all threads were interrupted
        for (int i = 0; i < numThreads; i++) {
            assertTrue("Thread " + i + " should have exception", exceptions[i].get() != null);
            assertTrue("Thread " + i + " should be interrupted",
                    exceptions[i].get() instanceof InterruptedException);
        }
    }

    /**
     * Test that shutdown is idempotent
     */
    @Test
    public void shouldHandleMultipleShutdownCalls() throws Exception {
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(5)
                .maxQueueSize(5)
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
                .pollInterval(Duration.ofMillis(100))
                .build();

        // Multiple shutdown calls should not cause issues
        queue.shutdown();
        queue.shutdown();
        queue.shutdown();

        // Enqueue on shutdown queue should fail immediately
        DataChangeEvent testEvent = createTestEvent();

        long startTime = System.currentTimeMillis();
        try {
            queue.enqueue(testEvent);
            assertTrue("Enqueue should fail on shutdown queue", false);
        }
        catch (InterruptedException e) {
            long duration = System.currentTimeMillis() - startTime;
            assertTrue("Should fail quickly", duration < 100);
            assertTrue("Should mention shutdown", e.getMessage().contains("shut down"));
        }
    }

    /**
     * Test that flushBuffer is also interrupted by shutdown
     */
    @Test
    public void shouldUnblockFlushBufferOnShutdown() throws Exception {
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(2)
                .maxQueueSize(2)
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
                .pollInterval(Duration.ofMillis(100))
                .buffering()
                .build();

        // Fill queue to capacity
        DataChangeEvent testEvent = createTestEvent();
        for (int i = 0; i < 2; i++) {
            queue.enqueue(testEvent);
        }

        AtomicReference<Exception> flushException = new AtomicReference<>();
        CountDownLatch flushStarted = new CountDownLatch(1);
        CountDownLatch flushFinished = new CountDownLatch(1);

        Thread flushThread = new Thread(() -> {
            try {
                flushStarted.countDown();
                // This should block because queue is full
                queue.flushBuffer(event -> event);
            }
            catch (Exception e) {
                flushException.set(e);
            }
            finally {
                flushFinished.countDown();
            }
        });

        flushThread.start();

        // Wait for thread to start
        assertTrue("Flush thread should start", flushStarted.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);

        // Shutdown should unblock the flush operation
        queue.shutdown();
        boolean flushFinishedInTime = flushFinished.await(5, TimeUnit.SECONDS);

        assertTrue("Flush should finish after shutdown", flushFinishedInTime);
        assertTrue("Flush should be interrupted", flushException.get() instanceof InterruptedException);
    }

    /**
     * Test that isRunning() correctly reflects queue state
     */
    @Test
    public void shouldReflectRunningState() throws Exception {
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(5)
                .maxQueueSize(5)
                .loggingContextSupplier(() -> LoggingContext.forConnector("test", "test", "test"))
                .pollInterval(Duration.ofMillis(100))
                .build();

        // Queue should be running initially
        assertTrue("Queue should be running initially", queue.isRunning());

        // After shutdown, should not be running
        queue.shutdown();
        assertFalse("Queue should not be running after shutdown", queue.isRunning());
    }

    /**
     * Helper method to create a test DataChangeEvent
     */
    private DataChangeEvent createTestEvent() {
        // Create a minimal SourceRecord for testing
        SourceRecord sourceRecord = new SourceRecord(
                null, // source partition
                null, // source offset
                "test-topic", // topic
                null, // partition
                null, // key schema
                null, // key
                null, // value schema
                "test-value" // value
        );
        return new DataChangeEvent(sourceRecord);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages threaded execution of snapshot work using a thread pool.
 *
 * <p>The pool is either created and owned by this executor, in which case {@link #close()} shuts it
 * down, or supplied by the caller, in which case it outlives this executor and only the submitted
 * tasks are tracked here.
 */
public class ThreadedSnapshotExecutor implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadedSnapshotExecutor.class);

    private final ExecutorService executorService;
    private final boolean ownsExecutorService;
    private final CompletionService<Void> completionService;
    private int submittedTasks = 0;

    public ThreadedSnapshotExecutor(int threadCount, String description) {
        this(createFixedThreadPool(threadCount, description), true);
    }

    public ThreadedSnapshotExecutor(ExecutorService executorService) {
        this(executorService, false);
    }

    private ThreadedSnapshotExecutor(ExecutorService executorService, boolean ownsExecutorService) {
        this.executorService = executorService;
        this.ownsExecutorService = ownsExecutorService;
        this.completionService = new ExecutorCompletionService<>(executorService);
    }

    private static ExecutorService createFixedThreadPool(int threadCount, String description) {
        LOGGER.info("Creating {} worker pool with {} worker thread(s)", description, threadCount);
        return Executors.newFixedThreadPool(threadCount);
    }

    public void submit(Callable<Void> task) {
        completionService.submit(task);
        submittedTasks++;
    }

    public void awaitCompletion() throws InterruptedException, ExecutionException {
        for (int i = 0; i < submittedTasks; i++) {
            completionService.take().get();
        }
    }

    @Override
    public void close() {
        if (ownsExecutorService) {
            executorService.shutdownNow();
        }
    }
}

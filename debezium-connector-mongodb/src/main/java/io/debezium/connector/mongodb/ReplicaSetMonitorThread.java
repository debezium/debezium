/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * A thread that can be used to when new replica sets are added or existing replica sets are removed. The logic does not evaluate
 * membership changes of individual replica sets, since that is handled independently by each task.
 * 
 * @author Randall Hauch
 */
public final class ReplicaSetMonitorThread extends Thread {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Metronome metronome;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch initialized = new CountDownLatch(1);
    private final Supplier<ReplicaSets> monitor;
    private final Consumer<ReplicaSets> onChange;
    private final Runnable onStartup;
    private volatile ReplicaSets replicaSets = ReplicaSets.empty();

    /**
     * @param monitor the component used to periodically obtain the replica set specifications; may not be null
     * @param period the time period between polling checks; must be non-negative
     * @param unit the time unit for the {@code period}; may not be null
     * @param clock the clock to use; may be null if the system clock should be used
     * @param onStartup the function to call when the thread is started; may be null if not needed
     * @param onChange the function to call when the set of replica set specifications has changed; may be null if not needed
     */
    public ReplicaSetMonitorThread(Supplier<ReplicaSets> monitor, long period, TimeUnit unit, Clock clock, Runnable onStartup,
            Consumer<ReplicaSets> onChange) {
        if (clock == null) clock = Clock.system();
        this.monitor = monitor;
        this.metronome = Metronome.sleeper(period, unit, clock);
        this.onChange = onChange != null ? onChange : (rsSpecs) -> {};
        this.onStartup = onStartup != null ? onStartup : () -> {};
    }

    @Override
    public void run() {
        if (running.compareAndSet(false, true)) {
            // We were not running, but we are now ...
            onStartup.run();
            while (running.get()) {
                try {
                    ReplicaSets previousReplicaSets = replicaSets;
                    replicaSets = monitor.get();
                    initialized.countDown();
                    // Determine if any replica set specifications have changed ...
                    if (replicaSets.haveChangedSince(previousReplicaSets)) {
                        // At least one of the replica sets been added or been removed ...
                        try {
                            onChange.accept(replicaSets);
                        } catch (Throwable t) {
                            logger.error("Error while calling the function with the new replica set specifications", t);
                        }
                    }
                } catch (Throwable t) {
                    logger.error("Error while trying to get information about the replica sets", t);
                }
                // Check again whether we are running before we pause ...
                if (running.get()) {
                    try {
                        metronome.pause();
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                    }
                }
            }
        }
    }

    /**
     * Requests that this thread stop running.
     */
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            logger.debug("Stopping the thread monitoring replica sets");
            // We were running, so interrupt the thread if it is paused ...
            try {
                this.interrupt();
            } catch (Throwable t) {
                logger.warn("Unable to interrupt the thread monitoring replica sets", t);
            }
        }
    }

    /**
     * Get the information about each of the replica sets.
     * 
     * @param timeout the time to block until the replica sets are first obtained from MongoDB; may not be negative
     * @param unit the time unit for the {@code timeout}; may not be null
     * @return the replica sets, or {@code null} if the timeout occurred before the replica set information was obtained
     */
    public ReplicaSets getReplicaSets(long timeout, TimeUnit unit) {
        try {
            if (initialized.await(timeout, unit)) {
                return replicaSets;
            }
        } catch (InterruptedException e) {
            Thread.interrupted(); // but do nothing else
        }
        return null;
    }
}

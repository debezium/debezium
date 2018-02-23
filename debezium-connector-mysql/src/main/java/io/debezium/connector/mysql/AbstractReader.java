/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.network.ServerException;

import io.debezium.config.ConfigurationDefaults;
import io.debezium.time.Temporals;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * A component that performs a snapshot of a MySQL server, and records the schema changes in {@link MySqlSchema}.
 *
 * @author Randall Hauch
 */
public abstract class AbstractReader implements Reader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final String name;
    protected final MySqlTaskContext context;
    protected final MySqlJdbcContext connectionContext;
    private final BlockingQueue<SourceRecord> records;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean success = new AtomicBoolean(false);
    private final AtomicReference<ConnectException> failure = new AtomicReference<>();
    private ConnectException failureException;
    private final int maxBatchSize;
    private final Metronome metronome;
    private final AtomicReference<Runnable> uponCompletion = new AtomicReference<>();
    private final Duration pollInterval;

    /**
     * Create a snapshot reader.
     *
     * @param name the name of the reader
     * @param context the task context in which this reader is running; may not be null
     */
    public AbstractReader(String name, MySqlTaskContext context) {
        this.name = name;
        this.context = context;
        this.connectionContext = context.getConnectionContext();
        this.records = new LinkedBlockingDeque<>(context.getConnectorConfig().getMaxQueueSize());
        this.maxBatchSize = context.getConnectorConfig().getMaxBatchSize();
        this.pollInterval = context.getConnectorConfig().getPollInterval();
        this.metronome = Metronome.parker(pollInterval, Clock.SYSTEM);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void uponCompletion(Runnable handler) {
        assert this.uponCompletion.get() == null;
        this.uponCompletion.set(handler);
    }

    @Override
    public final void initialize() {
        doInitialize();
    }

    @Override
    public void start() {
        if (this.running.compareAndSet(false, true)) {
            this.failure.set(null);
            this.success.set(false);
            doStart();
        }
    }

    @Override
    public void stop() {
        try {
            doStop();
            running.set(false);
        } finally {
            if (failure.get() != null) {
                // We had a failure and it was propagated via poll(), after which Kafka Connect will stop
                // the connector, which will stop the task that will then stop this reader via this method.
                // Since no more records will ever be polled again, we know we can clean up this reader's resources...
                doCleanup();
            }
        }
    }

    /**
     * The reader has been requested to initialize resources prior to starting. This should only be
     * called once before {@link #doStart()}.
     */
    protected void doInitialize() {
        // do nothing
    }

    /**
     * The reader has been requested to start, so initialize any un-initialized resources required
     * by the reader.
     */
    protected abstract void doStart();

    /**
     * The reader has been requested to stop, so perform any work required to stop the reader's resources that were previously
     * {@link #start() started}.
     * <p>
     * This method is always called when {@link #stop()} is called, and the first time {@link #isRunning()} will return
     * {@code true} the first time and {@code false} for any subsequent calls.
     */
    protected abstract void doStop();

    /**
     * The reader has completed all processing and all {@link #enqueueRecord(SourceRecord) enqueued records} have been
     * {@link #poll() consumed}, so this reader should clean up any resources that might remain.
     */
    protected abstract void doCleanup();

    /**
     * Call this method only when the reader has successfully completed all of its work, signaling that subsequent
     * calls to {@link #poll()} should forever return {@code null} and that this reader should transition from
     * {@link Reader.State#STOPPING} to {@link Reader.State#STOPPED}.
     */
    protected void completeSuccessfully() {
        this.success.set(true);
    }

    /**
     * Call this method only when the reader has failed, that a subsequent call to {@link #poll()} should throw
     * this error, and that {@link #doCleanup()} can be called at any time.
     *
     * @param error the error that resulted in the failure; should not be {@code null}
     */
    protected void failed(Throwable error) {
        this.failure.set(wrap(error));
    }

    /**
     * Call this method only when the reader has failed, that a subsequent call to {@link #poll()} should throw
     * this error, and that {@link #doCleanup()} can be called at any time.
     *
     * @param error the error that resulted in the failure; should not be {@code null}
     * @param msg the error message; may not be null
     */
    protected void failed(Throwable error, String msg) {
        ConnectException wrapped = wrap(error);
        this.logger.error("Failed due to error: {}", msg, wrapped);
        this.failure.set(wrapped);
    }

    /**
     * Wraps the specified exception in a {@link ConnectException}, ensuring that all useful state is captured inside
     * the new exception's message.
     *
     * @param error the exception; may not be null
     * @return the wrapped Kafka Connect exception
     */
    protected ConnectException wrap(Throwable error) {
        assert error != null;
        String msg = error.getMessage();
        if (error instanceof ServerException) {
            ServerException e = (ServerException) error;
            msg = msg + " Error code: " + e.getErrorCode() + "; SQLSTATE: " + e.getSqlState() + ".";
        } else if (error instanceof SQLException) {
            SQLException e = (SQLException) error;
            msg = e.getMessage() + " Error code: " + e.getErrorCode() + "; SQLSTATE: " + e.getSQLState() + ".";
        }
        return new ConnectException(msg, error);
    }

    @Override
    public State state() {
        if (success.get() || failure.get() != null) {
            // We've either completed successfully or have failed, but either way no more records will be returned ...
            return State.STOPPED;
        }
        if (running.get()) {
            return State.RUNNING;
        }
        // Otherwise, we're in the process of stopping ...
        return State.STOPPING;
    }

    protected boolean isRunning() {
        return running.get();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // Before we do anything else, determine if there was a failure and throw that exception ...
        failureException = this.failure.get();
        if (failureException != null) {
            // In this case, we'll throw the exception and the Kafka Connect worker or EmbeddedEngine
            // will then explicitly stop the connector task. Most likely, however, the reader that threw
            // the exception will have already stopped itself and will generate no additional records.
            // Regardless, there may be records on the queue that will never be consumed.
            throw failureException;
        }

        // this reader has been stopped before it reached the success or failed end state, so clean up and abort
        if (!running.get()) {
            cleanupResources();
            throw new InterruptedException( "Reader was stopped while polling" );
        }

        logger.trace("Polling for next batch of records");
        List<SourceRecord> batch = new ArrayList<>(maxBatchSize);
        final Timer timeout = Threads.timer(Clock.SYSTEM, Temporals.max(pollInterval, ConfigurationDefaults.RETURN_CONTROL_INTERVAL));
        while (running.get() && (records.drainTo(batch, maxBatchSize) == 0) && !success.get()) {
            // No records are available even though the snapshot has not yet completed, so sleep for a bit ...
            metronome.pause();

            // Check for failure after waking up ...
            failureException = this.failure.get();
            if (failureException != null) throw failureException;
            if (timeout.expired()) {
                break;
            }
        }

        if (batch.isEmpty() && success.get() && records.isEmpty()) {
            // We found no records but the operation completed successfully, so we're done
            this.running.set(false);
            cleanupResources();
            return null;
        }
        pollComplete(batch);
        logger.trace("Completed batch of {} records", batch.size());
        return batch;
    }

    /**
     * This method is normally called by {@link #poll()} when there this reader finishes normally and all generated
     * records are consumed prior to being {@link #stop() stopped}. However, if this reader is explicitly
     * {@link #stop() stopped} while still working, then subclasses should call this method when they have completed
     * all of their shutdown work.
     */
    protected void cleanupResources() {
        try {
            doCleanup();
        } finally {
            Runnable completionHandler = uponCompletion.getAndSet(null); // set to null so that we call it only once
            if (completionHandler != null) {
                completionHandler.run();
            }
        }
    }

    /**
     * Method called when {@link #poll()} completes sending a non-zero-sized batch of records.
     *
     * @param batch the batch of records being recorded
     */
    protected void pollComplete(List<SourceRecord> batch) {
        // do nothing
    }

    /**
     * Enqueue a record so that it can be obtained when this reader is {@link #poll() polled}. This method will block if the
     * queue is full.
     *
     * @param record the record to be enqueued
     * @throws InterruptedException if interrupted while waiting for the queue to have room for this record
     */
    protected void enqueueRecord(SourceRecord record) throws InterruptedException {
        if (record != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Enqueuing source record: {}", record);
            }
            this.records.put(record);
        }
    }
}

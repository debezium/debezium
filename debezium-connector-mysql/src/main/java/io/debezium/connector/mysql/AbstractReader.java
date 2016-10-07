/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.network.ServerException;

import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * A component that performs a snapshot of a MySQL server, and records the schema changes in {@link MySqlSchema}.
 * 
 * @author Randall Hauch
 */
public abstract class AbstractReader {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final MySqlTaskContext context;
    private final BlockingQueue<SourceRecord> records;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean success = new AtomicBoolean(false);
    private final AtomicReference<ConnectException> failure = new AtomicReference<>();
    private ConnectException failureException;
    private final int maxBatchSize;
    private final Metronome metronome;

    /**
     * Create a snapshot reader.
     * 
     * @param context the task context in which this reader is running; may not be null
     */
    public AbstractReader(MySqlTaskContext context) {
        this.context = context;
        this.records = new LinkedBlockingDeque<>(context.maxQueueSize());
        this.maxBatchSize = context.maxBatchSize();
        this.metronome = Metronome.parker(context.pollIntervalInMillseconds(), TimeUnit.MILLISECONDS, Clock.SYSTEM);
    }

    /**
     * Start the reader and return immediately. Once started, the {@link SourceRecord} can be obtained by periodically calling
     * {@link #poll()} until that method returns {@code null}.
     * <p>
     * This method does nothing if it is already running.
     */
    public void start() {
        if (this.running.compareAndSet(false, true)) {
            this.failure.set(null);
            this.success.set(false);
            doStart();
        }
    }

    /**
     * Stop the snapshot from running.
     * <p>
     * This method does nothing if the snapshot is not {@link #isRunning() running}.
     */
    public void stop() {
        if (this.running.compareAndSet(true, false)) {
            doStop();
        }
    }

    /**
     * The reader has been requested to start, so initialize any resources required by the reader.
     */
    protected abstract void doStart();

    /**
     * The reader has been requested to stop, so perform any work required to stop the reader's resources that were previously
     * {@link #start() started}..
     */
    protected abstract void doStop();

    /**
     * The reader has completed sending all {@link #enqueueRecord(SourceRecord) enqueued records}, so clean up any resources
     * that remain.
     */
    protected abstract void doCleanup();

    /**
     * Call this method only when the reader has successfully completed all of its work, signaling that subsequent
     * calls to {@link #poll()} should forever return {@code null}.
     */
    protected void completeSuccessfully() {
        this.success.set(true);
    }

    /**
     * Call this method only when the reader has failed, and that a subsequent call to {@link #poll()} should throw this error.
     * 
     * @param error the error that resulted in the failure; should not be {@code null}
     */
    protected void failed(Throwable error) {
        this.failure.set(wrap(error));
    }

    /**
     * Call this method only when the reader has failed, and that a subsequent call to {@link #poll()} should throw this error.
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

    /**
     * Get whether the snapshot is still running and records are available.
     * 
     * @return {@code true} if still running, or {@code false} if no longer running and/or all records have been processed
     */
    public boolean isRunning() {
        return this.running.get();
    }

    /**
     * Poll for the next batch of source records. This method blocks if this reader is still running but no records are available.
     * 
     * @return the list of source records; or {@code null} when the snapshot is complete, all records have previously been
     *         returned, and the completion function (supplied in the constructor) has been called
     * @throws InterruptedException if this thread is interrupted while waiting for more records
     * @throws ConnectException if there is an error while this reader is running
     */
    public List<SourceRecord> poll() throws InterruptedException {
        failureException = this.failure.get();
        if (failureException != null) throw failureException;

        logger.trace("Polling for next batch of records");
        List<SourceRecord> batch = new ArrayList<>(maxBatchSize);
        while (running.get() && (records.drainTo(batch, maxBatchSize) == 0) && !success.get()) {
            // No records are available even though the snapshot has not yet completed, so sleep for a bit ...
            metronome.pause();

            // Check for failure after waking up ...
            failureException = this.failure.get();
            if (failureException != null) throw failureException;
        }

        if (batch.isEmpty() && success.get() && records.isEmpty()) {
            // We found no records but the operation completed successfully, so we're done
            this.running.set(false);
            doCleanup();
            return null;
        }
        pollComplete(batch);
        logger.trace("Completed batch of {} records", batch.size());
        return batch;
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
        if (record != null) this.records.put(record);
    }
}

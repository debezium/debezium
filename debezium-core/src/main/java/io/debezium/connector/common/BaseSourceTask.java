/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Metronome;

/**
 * Base class for Debezium's CDC {@link SourceTask} implementations. Provides functionality common to all connectors,
 * such as validation of the configuration.
 *
 * @author Gunnar Morling
 */
public abstract class BaseSourceTask extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseSourceTask.class);

    protected static enum State {
        RUNNING,
        STOPPED;
    }

    private final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);

    /**
     * Used to ensure that start(), stop() and commitRecord() calls are serialized.
     */
    private final ReentrantLock stateLock = new ReentrantLock();

    private volatile ElapsedTimeStrategy restartDelay;

    /**
     * Raw connector properties, kept here so they can be passed again in case of a restart.
     */
    private volatile Map<String, String> props;

    /**
     * The change event source coordinator for those connectors adhering to the new
     * framework structure, {@code null} for legacy-style connectors.
     */
    private ChangeEventSourceCoordinator coordinator;

    /**
     * The latest offset that has been acknowledged by the Kafka producer. Will be
     * acknowledged with the source database in {@link BaseSourceTask#commit()}
     * (which may be a no-op depending on the connector).
     */
    private volatile Map<String, ?> lastOffset;

    private Duration retriableRestartWait;

    @Override
    public final void start(Map<String, String> props) {
        if (context == null) {
            throw new ConnectException("Unexpected null context");
        }

        stateLock.lock();

        try {
            if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
                LOGGER.info("Connector has already been started");
                return;
            }

            this.props = props;
            Configuration config = Configuration.from(props);
            retriableRestartWait = config.getDuration(CommonConnectorConfig.RETRIABLE_RESTART_WAIT, ChronoUnit.MILLIS);
            // need to reset the delay or you only get one delayed restart
            restartDelay = null;
            if (!config.validateAndRecord(getAllConfigurationFields(), LOGGER::error)) {
                throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Starting {} with configuration:", getClass().getSimpleName());
                config.withMaskedPasswords().forEach((propName, propValue) -> {
                    LOGGER.info("   {} = {}", propName, propValue);
                });
            }

            this.coordinator = start(config);
        }
        finally {
            stateLock.unlock();
        }
    }

    /**
     * Called once when starting this source task.
     *
     * @param config
     *            the task configuration; implementations should wrap it in a dedicated implementation of
     *            {@link CommonConnectorConfig} and work with typed access to configuration properties that way
     */
    protected abstract ChangeEventSourceCoordinator start(Configuration config);

    @Override
    public final List<SourceRecord> poll() throws InterruptedException {
        boolean started = startIfNeededAndPossible();

        // in backoff period after a retriable exception
        if (!started) {
            // WorkerSourceTask calls us immediately after we return the empty list.
            // This turns into a throttling so we need to make a pause before we return
            // the control back.
            Metronome.parker(Duration.of(2, ChronoUnit.SECONDS), Clock.SYSTEM).pause();
            return Collections.emptyList();
        }

        try {
            return doPoll();
        }
        catch (RetriableException e) {
            stop(true);
            throw e;
        }
    }

    /**
     * Returns the next batch of source records, if any are available.
     */
    protected abstract List<SourceRecord> doPoll() throws InterruptedException;

    /**
     * Starts this connector in case it has been stopped after a retriable error,
     * and the backoff period has passed.
     */
    private boolean startIfNeededAndPossible() {
        stateLock.lock();

        try {
            if (state.get() == State.RUNNING) {
                return true;
            }
            else if (restartDelay != null && restartDelay.hasElapsed()) {
                start(props);
                return true;
            }
            else {
                LOGGER.info("Awaiting end of restart backoff period after a retriable error");
                return false;
            }
        }
        finally {
            stateLock.unlock();
        }
    }

    @Override
    public final void stop() {
        stop(false);
    }

    private void stop(boolean restart) {
        stateLock.lock();

        try {
            if (!state.compareAndSet(State.RUNNING, State.STOPPED)) {
                LOGGER.info("Connector has already been stopped");
                return;
            }

            if (restart) {
                LOGGER.warn("Going to restart connector after {} sec. after a retriable exception", retriableRestartWait.getSeconds());
            }
            else {
                LOGGER.info("Stopping down connector");
            }

            try {
                if (coordinator != null) {
                    coordinator.stop();
                }
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                LOGGER.error("Interrupted while stopping coordinator", e);
                throw new ConnectException("Interrupted while stopping coordinator, failing the task");
            }

            doStop();

            if (restart && restartDelay == null) {
                restartDelay = ElapsedTimeStrategy.constant(Clock.system(), retriableRestartWait.toMillis());
                restartDelay.hasElapsed();
            }
        }
        finally {
            stateLock.unlock();
        }
    }

    protected abstract void doStop();

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        Map<String, ?> currentOffset = record.sourceOffset();
        if (currentOffset != null) {
            this.lastOffset = currentOffset;
        }
    }

    @Override
    public void commit() throws InterruptedException {
        boolean locked = stateLock.tryLock();

        if (locked) {
            try {
                if (coordinator != null && lastOffset != null) {
                    coordinator.commitOffset(lastOffset);
                }
            }
            finally {
                stateLock.unlock();
            }
        }
        else {
            LOGGER.warn("Couldn't commit processed log positions with the source database due to a concurrent connector shutdown or restart");
        }
    }

    /**
     * Returns all configuration {@link Field} supported by this source task.
     */
    protected abstract Iterable<Field> getAllConfigurationFields();

    /**
     * Loads the connector's persistent offset (if present) via the given loader.
     */
    protected OffsetContext getPreviousOffset(OffsetContext.Loader loader) {
        Map<String, ?> partition = loader.getPartition();

        if (lastOffset != null) {
            OffsetContext offsetContext = loader.load(lastOffset);
            LOGGER.info("Found previous offset after restart {}", offsetContext);
            return offsetContext;
        }

        Map<String, Object> previousOffset = context.offsetStorageReader()
                .offsets(Collections.singleton(partition))
                .get(partition);

        if (previousOffset != null) {
            OffsetContext offsetContext = loader.load(previousOffset);
            LOGGER.info("Found previous offset {}", offsetContext);
            return offsetContext;
        }
        else {
            return null;
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.SingleThreadAccess;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.function.LogPositionValidator;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.notification.channels.NotificationChannel;
import io.debezium.pipeline.signal.channels.SignalChannelReader;
import io.debezium.pipeline.signal.channels.process.SignalChannelWriter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.processors.PostProcessorRegistryServiceProvider;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.SnapshotQueryProvider;
import io.debezium.snapshot.SnapshotterServiceProvider;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * Base class for Debezium's CDC {@link SourceTask} implementations. Provides functionality common to all connectors,
 * such as validation of the configuration.
 *
 * @author Gunnar Morling
 */
public abstract class BaseSourceTask<P extends Partition, O extends OffsetContext> extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseSourceTask.class);
    private static final Duration INITIAL_POLL_PERIOD_IN_MILLIS = Duration.ofMillis(TimeUnit.SECONDS.toMillis(5));
    private static final Duration MAX_POLL_PERIOD_IN_MILLIS = Duration.ofMillis(TimeUnit.HOURS.toMillis(1));
    private Configuration config;
    private List<SignalChannelReader> signalChannels;

    protected void validateAndLoadSchemaHistory(CommonConnectorConfig config, LogPositionValidator logPositionValidator, Offsets<P, O> previousOffsets,
                                                DatabaseSchema schema,
                                                Snapshotter snapshotter) {

        for (Map.Entry<P, O> previousOffset : previousOffsets) {

            Partition partition = previousOffset.getKey();
            OffsetContext offset = previousOffset.getValue();

            if (offset == null) {
                if (snapshotter.shouldSnapshotOnSchemaError()) {
                    // We are in schema only recovery mode, use the existing redo log position
                    // would like to also verify redo log position exists, but it defaults to 0 which is technically valid
                    throw new DebeziumException("Could not find existing redo log information while attempting schema only recovery snapshot");
                }
                LOGGER.info("Connector started for the first time.");
                if (schema.isHistorized()) {
                    ((HistorizedDatabaseSchema) schema).initializeStorage();
                }
                return;
            }

            if (offset.isInitialSnapshotRunning()) {
                // The last offset was an incomplete snapshot and now the snapshot was disabled
                if (!snapshotter.shouldSnapshotData(true, true) &&
                        !snapshotter.shouldSnapshotSchema(true, true)) {
                    // No snapshots are allowed
                    throw new DebeziumException("The connector previously stopped while taking a snapshot, but now the connector is configured "
                            + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.");
                }
            }
            else {

                if (schema.isHistorized() && !((HistorizedDatabaseSchema) schema).historyExists()) {

                    LOGGER.warn("Database schema history was not found but was expected");

                    if (snapshotter.shouldSnapshotOnSchemaError()) {

                        LOGGER.info("The db-history topic is missing but we are in {} snapshot mode. " +
                                "Attempting to snapshot the current schema and then begin reading the redo log from the last recorded offset.",
                                snapshotter.name());
                        if (schema.isHistorized()) {
                            ((HistorizedDatabaseSchema) schema).initializeStorage();
                        }
                        return;
                    }
                    else {
                        throw new DebeziumException("The db history topic is missing. You may attempt to recover it by reconfiguring the connector to recovery.");
                    }
                }

                if (config.isLogPositionCheckEnabled()) {

                    boolean logPositionAvailable = isLogPositionAvailable(logPositionValidator, partition, offset, config);

                    if (!logPositionAvailable) {
                        LOGGER.warn("Last recorded offset is no longer available on the server.");

                        if (snapshotter.shouldSnapshotOnDataError()) {

                            LOGGER.info("The last recorded offset is no longer available but we are in {} snapshot mode. " +
                                    "Attempting to snapshot data to fill the gap.",
                                    snapshotter.name());

                            previousOffsets.resetOffset(previousOffsets.getTheOnlyPartition());

                            return;
                        }

                        LOGGER.warn("The connector is trying to read redo log starting at " + offset + ", but this is no longer "
                                + "available on the server. Reconfigure the connector to use a snapshot when needed if you want to recover. " +
                                "If not the connector will streaming from the last available position in the log");
                    }
                }

                if (schema.isHistorized()) {
                    ((HistorizedDatabaseSchema) schema).recover(partition, offset);
                }
            }
        }
    }

    public boolean isLogPositionAvailable(LogPositionValidator logPositionValidator, Partition partition, OffsetContext offsetContext, CommonConnectorConfig config) {

        if (logPositionValidator == null) {
            LOGGER.warn("Current JDBC connection implementation is not providing a log position validator implementation. The check will always be 'true'");
            return true;
        }
        return logPositionValidator.validate(partition, offsetContext, config);
    }

    public enum State {
        RESTARTING,
        RUNNING,
        INITIAL,
        STOPPED
    }

    private final AtomicReference<State> state = new AtomicReference<>(State.INITIAL);

    /**
     * Used to ensure that start(), stop() and commitRecord() calls are serialized.
     */
    private final ReentrantLock stateLock = new ReentrantLock();

    private volatile ElapsedTimeStrategy restartDelay;

    /**
     * The change event source coordinator for those connectors adhering to the new
     * framework structure, {@code null} for legacy-style connectors.
     */
    protected ChangeEventSourceCoordinator<P, O> coordinator;

    /**
     * The latest offsets that have been acknowledged by the Kafka producer. Will be
     * acknowledged with the source database in {@link BaseSourceTask#commit()}
     * (which may be a no-op depending on the connector).
     */
    private final Map<Map<String, ?>, Map<String, ?>> lastOffsets = new HashMap<>();

    private Duration retriableRestartWait;

    private final ElapsedTimeStrategy pollOutputDelay;

    private final Clock clock = Clock.system();
    @SingleThreadAccess("polling thread")
    private Instant previousOutputInstant;

    @SingleThreadAccess("polling thread")
    private int previousOutputBatchSize;

    private final ServiceLoader<SignalChannelReader> availableSignalChannels = ServiceLoader.load(SignalChannelReader.class);

    private final List<NotificationChannel> notificationChannels;

    /**
     * A flag to record whether the offsets stored in the offset store are loaded for the first time.
     * This is typically used to reduce logging in case a connector like PostgreSQL reads offsets
     * not only on connector startup but repeatedly during execution time too.
     */
    private boolean offsetLoadedInPast = false;

    protected BaseSourceTask() {
        // Use exponential delay to log the progress frequently at first, but the quickly tapering off to once an hour...
        pollOutputDelay = ElapsedTimeStrategy.exponential(clock, INITIAL_POLL_PERIOD_IN_MILLIS, MAX_POLL_PERIOD_IN_MILLIS);
        previousOutputInstant = clock.currentTimeAsInstant();

        this.notificationChannels = StreamSupport.stream(ServiceLoader.load(NotificationChannel.class).spliterator(), false)
                .collect(Collectors.toList());
    }

    @Override
    public final void start(Map<String, String> props) {
        if (context == null) {
            throw new ConnectException("Unexpected null context");
        }

        stateLock.lock();

        try {
            setTaskState(State.INITIAL);
            config = Configuration.from(props);
            retriableRestartWait = config.getDuration(CommonConnectorConfig.RETRIABLE_RESTART_WAIT, ChronoUnit.MILLIS);
            // need to reset the delay or you only get one delayed restart
            restartDelay = null;
            if (!config.validateAndRecord(getAllConfigurationFields(), LOGGER::error)) {
                throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
            }

            if (LOGGER.isInfoEnabled()) {
                StringBuilder configLogBuilder = new StringBuilder("Starting " + getClass().getSimpleName() + " with configuration:");
                withMaskedSensitiveOptions(config).forEach((propName, propValue) -> {
                    configLogBuilder.append("\n   ").append(propName).append(" = ").append(propValue);
                });
                configLogBuilder.append("\n");
                LOGGER.info(configLogBuilder.toString());
            }
            try {
                this.coordinator = start(config);
                setTaskState(State.RUNNING);
            }
            catch (RetriableException e) {
                LOGGER.warn("Failed to start connector, will re-attempt during polling.", e);
                restartDelay = ElapsedTimeStrategy.constant(Clock.system(), retriableRestartWait);
                setTaskState(State.RESTARTING);
            }
        }
        finally {
            stateLock.unlock();
        }
    }

    /**
     * Returns the available signal channels.
     * <p>
     *     The signal channels are loaded using the {@link ServiceLoader} mechanism and cached for the lifetime of the task.
     * </p>
     *
     * @return list of loaded signal channels
     */
    public List<SignalChannelReader> getAvailableSignalChannels() {
        if (signalChannels == null) {
            signalChannels = availableSignalChannels.stream().map(ServiceLoader.Provider::get).collect(Collectors.toList());
        }
        return signalChannels;
    }

    /**
     * Returns the first available signal channel writer
     *
     * @return the first available signal channel writer empty optional if not available
     */
    public Optional<? extends SignalChannelWriter> getAvailableSignalChannelWriter() {
        return getAvailableSignalChannels().stream()
                .filter(SignalChannelWriter.class::isInstance)
                .map(SignalChannelWriter.class::cast)
                .findFirst();
    }

    protected Configuration withMaskedSensitiveOptions(Configuration config) {
        return config.withMaskedPasswords();
    }

    /**
     * Called when starting this source task.  This method can throw a {@link RetriableException} to indicate
     * that the task should attempt to retry the start later.
     *
     * @param config
     *            the task configuration; implementations should wrap it in a dedicated implementation of
     *            {@link CommonConnectorConfig} and work with typed access to configuration properties that way
     */
    protected abstract ChangeEventSourceCoordinator<P, O> start(Configuration config);

    @Override
    public final List<SourceRecord> poll() throws InterruptedException {

        try {
            // in we fail to start, return empty list and try to start next poll() method call
            if (!startIfNeededAndPossible()) {
                return Collections.emptyList();
            }

            final List<SourceRecord> records = doPoll();
            logStatistics(records);

            resetErrorHandlerRetriesIfNeeded(records);

            return records;
        }
        catch (RetriableException e) {
            stop(true);
            throw e;
        }
    }

    protected void logStatistics(final List<SourceRecord> records) {
        if (records == null || !LOGGER.isInfoEnabled()) {
            return;
        }
        int batchSize = records.size();

        if (batchSize > 0) {
            // We want to log the number of records per topic...
            if (LOGGER.isDebugEnabled()) {
                final Map<String, Integer> topicCounts = new LinkedHashMap<>();
                records.forEach(r -> topicCounts.merge(r.topic(), 1, Integer::sum));
                for (Map.Entry<String, Integer> topicCount : topicCounts.entrySet()) {
                    LOGGER.debug("Sending {} records to topic {}", topicCount.getValue(), topicCount.getKey());
                }
            }

            SourceRecord lastRecord = records.get(batchSize - 1);
            previousOutputBatchSize += batchSize;
            if (pollOutputDelay.hasElapsed()) {
                // We want to record the status ...
                final Instant currentTime = clock.currentTime();
                LOGGER.info("{} records sent during previous {}, last recorded offset of {} partition is {}", previousOutputBatchSize,
                        Strings.duration(Duration.between(previousOutputInstant, currentTime).toMillis()),
                        lastRecord.sourcePartition(), lastRecord.sourceOffset());

                previousOutputInstant = currentTime;
                previousOutputBatchSize = 0;
            }
        }
    }

    private void updateLastOffset(Map<String, ?> partition, Map<String, ?> lastOffset) {
        stateLock.lock();
        lastOffsets.put(partition, lastOffset);
        stateLock.unlock();
    }

    /**
     * Should be called to reset the error handler's retry counter upon a successful poll or when known
     * that the connector task has recovered from a previous failure state.
     */
    protected void resetErrorHandlerRetriesIfNeeded(List<SourceRecord> records) {
        // When a connector throws a retriable error, the task is not re-created and instead the previous
        // error handler is passed into the new error handler, propagating the retry count. This method
        // allows resetting that counter when a successful poll iteration step contains new records so that when a
        // future failure is thrown, the maximum retry count can be utilized.
        if (containsMessages(records) && coordinator != null && coordinator.getErrorHandler().getRetries() > 0) {
            coordinator.getErrorHandler().resetRetries();
        }
    }

    private boolean containsMessages(List<SourceRecord> records) {
        if (records == null || records.isEmpty()) {
            return false;
        }

        for (SourceRecord record : records) {
            if (record.valueSchema() != null && record.valueSchema().field(Envelope.FieldName.AFTER) != null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the next batch of source records, if any are available.
     */
    protected abstract List<SourceRecord> doPoll() throws InterruptedException;

    /**
     * Starts this connector in case it has been stopped after a retriable error,
     * and the backoff period has passed.
     */
    private boolean startIfNeededAndPossible() throws InterruptedException {
        stateLock.lock();

        boolean result = false;
        try {
            State currentState = getTaskState();
            if (currentState == State.RUNNING) {
                result = true;
            }
            else if (currentState == State.RESTARTING) {
                // we're in restart mode... check if it's time to restart
                if (restartDelay.hasElapsed()) {
                    LOGGER.info("Attempting to restart task.");
                    this.coordinator = start(config);
                    LOGGER.info("Successfully restarted task");
                    restartDelay = null;
                    setTaskState(State.RUNNING);
                    result = true;
                }
                else {
                    LOGGER.info("Awaiting end of restart backoff period after a retriable error");
                    Metronome.parker(retriableRestartWait, Clock.SYSTEM).pause();
                }
            }
        }
        finally {
            stateLock.unlock();
        }
        return result;
    }

    @Override
    public final void stop() {
        stop(false);
    }

    private void stop(boolean restart) {
        stateLock.lock();

        try {
            if (restart) {
                LOGGER.warn("Going to restart connector after {} sec. after a retriable exception", retriableRestartWait.getSeconds());
            }
            else {
                LOGGER.info("Stopping down connector");
            }

            try {
                if (coordinator != null) {
                    coordinator.stop();
                    coordinator = null;
                }
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                LOGGER.error("Interrupted while stopping coordinator", e);
                throw new ConnectException("Interrupted while stopping coordinator, failing the task");
            }

            doStop();

            if (restart) {
                setTaskState(State.RESTARTING);
                if (restartDelay == null) {
                    restartDelay = ElapsedTimeStrategy.constant(Clock.system(), retriableRestartWait);
                }
            }
            else {
                setTaskState(State.STOPPED);
            }
        }
        finally {
            stateLock.unlock();
        }
    }

    protected abstract void doStop();

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        LOGGER.trace("Committing record {}", maybeRedactSensitiveData(record));

        Map<String, ?> currentOffset = record.sourceOffset();
        if (currentOffset != null) {
            updateLastOffset(record.sourcePartition(), currentOffset);
        }
    }

    @Override
    public void commit() throws InterruptedException {
        boolean locked = stateLock.tryLock();

        if (locked) {
            try {
                if (coordinator != null) {
                    Iterator<Map<String, ?>> iterator = lastOffsets.keySet().iterator();
                    while (iterator.hasNext()) {
                        Map<String, ?> partition = iterator.next();
                        Map<String, ?> lastOffset = lastOffsets.get(partition);

                        LOGGER.debug("Committing offset '{}' for partition '{}'", partition, lastOffset);
                        coordinator.commitOffset(partition, lastOffset);
                        iterator.remove();
                    }
                }
            }
            finally {
                stateLock.unlock();
            }
        }
        else {
            LOGGER.info("Couldn't commit processed log positions with the source database due to a concurrent connector shutdown or restart");
        }
    }

    /**
     * Returns all configuration {@link Field} supported by this source task.
     */
    protected abstract Iterable<Field> getAllConfigurationFields();

    /**
     * Loads the connector's persistent offsets (if present) via the given loader.
     */
    protected Offsets<P, O> getPreviousOffsets(Partition.Provider<P> provider, OffsetContext.Loader<O> loader) {
        Set<P> partitions = provider.getPartitions();
        OffsetReader<P, O, OffsetContext.Loader<O>> reader = new OffsetReader<>(
                context.offsetStorageReader(), loader);
        Map<P, O> offsets = reader.offsets(partitions);

        boolean found = false;
        for (P partition : partitions) {
            O offset = offsets.get(partition);

            if (offset != null) {
                found = true;
                if (offsetLoadedInPast) {
                    LOGGER.debug("Found previous partition offset {}: {}", partition, offset.getOffset());
                }
                else {
                    LOGGER.info("Found previous partition offset {}: {}", partition, offset.getOffset());
                    offsetLoadedInPast = true;
                }
            }
        }

        if (!found) {
            LOGGER.info("No previous offsets found");
        }

        return Offsets.of(offsets);
    }

    /**
     * Sets the new state for the task. The caller must be holding {@link #stateLock} lock.
     *
     * @param newState
     */
    private void setTaskState(State newState) {
        State oldState = state.getAndSet(newState);
        LOGGER.debug("Setting task state to '{}', previous state was '{}'", newState, oldState);
    }

    @VisibleForTesting
    public State getTaskState() {
        stateLock.lock();
        try {
            return state.get();
        }
        finally {
            stateLock.unlock();
        }
    }

    public List<NotificationChannel> getNotificationChannels() {
        return notificationChannels;
    }

    protected void registerServiceProviders(ServiceRegistry serviceRegistry) {
        serviceRegistry.registerServiceProvider(new PostProcessorRegistryServiceProvider());
        serviceRegistry.registerServiceProvider(new SnapshotLockProvider());
        serviceRegistry.registerServiceProvider(new SnapshotQueryProvider());
        serviceRegistry.registerServiceProvider(new SnapshotterServiceProvider());
    }
}

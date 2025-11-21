/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.channels.SignalChannelReader;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Threads;

/**
 * This class permits to process signals coming from the different channels.
 *
 * @author Mario Fiore Vitale
 */
public class SignalProcessor<P extends Partition, O extends OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalProcessor.class);

    public static final int SEMAPHORE_WAIT_TIME = 10;

    private final Map<String, SignalAction<P>> signalActions = new HashMap<>();

    private final CommonConnectorConfig connectorConfig;

    private final List<SignalChannelReader> enabledChannelReaders;

    private final List<SignalChannelReader> signalChannelReaders;

    private final ScheduledExecutorService signalProcessorExecutor;

    private final DocumentReader documentReader;

    private final Map<P, O> partitionOffsets = new ConcurrentHashMap<>();

    private final Semaphore semaphore = new Semaphore(1);

    public SignalProcessor(Class<? extends SourceConnector> connector,
                           CommonConnectorConfig config,
                           Map<String, SignalAction<P>> signalActions,
                           List<SignalChannelReader> signalChannelReaders, DocumentReader documentReader,
                           Offsets<P, O> previousOffsets) {

        this.connectorConfig = config;
        this.signalChannelReaders = signalChannelReaders;
        this.documentReader = documentReader;
        if (previousOffsets != null) {
            previousOffsets.getOffsets().entrySet().stream()
                    .filter(entry -> entry.getValue() != null)
                    .forEach(entry -> this.partitionOffsets.put(entry.getKey(), entry.getValue()));
        }
        this.signalProcessorExecutor = Threads.newSingleThreadScheduledExecutor(connector, config.getLogicalName(), SignalProcessor.class.getSimpleName(), false);

        LOGGER.info("SignalProcessor initialized with {} partition(s): {}", partitionOffsets.size(), partitionOffsets.keySet());

        // filter single channel reader based on configuration
        this.enabledChannelReaders = getEnabledChannelReaders();

        // initialize single channel reader with connector config
        this.enabledChannelReaders.forEach(signalChannelReader -> signalChannelReader.init(connectorConfig));

        this.signalActions.putAll(signalActions);
    }

    private Predicate<SignalChannelReader> isEnabled() {
        return reader -> connectorConfig.getEnabledChannels().contains(reader.name());
    }

    private List<SignalChannelReader> getEnabledChannelReaders() {
        return signalChannelReaders.stream()
                .filter(isEnabled())
                .collect(Collectors.toList());
    }

    public void setContext(Offsets<P, O> offsets) {
        partitionOffsets.clear();
        if (offsets != null) {
            offsets.getOffsets().entrySet().stream()
                    .filter(entry -> entry.getValue() != null)
                    .forEach(entry -> partitionOffsets.put(entry.getKey(), entry.getValue()));
        }
        LOGGER.debug("Updated offset contexts for {} partition(s)", partitionOffsets.size());
    }

    public void start() {

        LOGGER.info("SignalProcessor started. Scheduling it every {}ms", connectorConfig.getSignalPollInterval().toMillis());
        signalProcessorExecutor.scheduleAtFixedRate(this::process, 0, connectorConfig.getSignalPollInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() throws InterruptedException {

        // The close must run with same thread of the read otherwise Kafka client will detect multi-thread and throw and exception
        signalProcessorExecutor.submit(() -> enabledChannelReaders
                .forEach(SignalChannelReader::close));

        signalProcessorExecutor.shutdown();
        boolean isShutdown = signalProcessorExecutor.awaitTermination(connectorConfig.getExecutorShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);

        if (!isShutdown) {
            LOGGER.warn("SignalProcessor didn't stop in the expected time, shutting down executor now");

            // Clear interrupt flag so the forced termination is always attempted
            Thread.interrupted();
            signalProcessorExecutor.shutdownNow();
            signalProcessorExecutor.awaitTermination(connectorConfig.getExecutorShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
        }

        LOGGER.info("SignalProcessor stopped");
    }

    public void registerSignalAction(String id, SignalAction<P> signal) {

        LOGGER.debug("Registering signal '{}' using class '{}'", id, signal.getClass().getName());
        signalActions.put(id, signal);
    }

    public void process() {

        executeWithSemaphore(() -> {
            LOGGER.trace("SignalProcessor processing");
            enabledChannelReaders.stream()
                    .map(SignalChannelReader::read)
                    .flatMap(Collection::stream)
                    .forEach(signalRecord -> processSignal(signalRecord, null));
        });
    }

    public void processSourceSignal(P partition) {

        executeWithSemaphore(() -> {
            LOGGER.trace("Processing source signals for partition {}", partition);
            enabledChannelReaders.stream()
                    .filter(isSignal(SourceSignalChannel.class))
                    .map(SignalChannelReader::read)
                    .flatMap(Collection::stream)
                    .forEach(signalRecord -> processSignal(signalRecord, partition));
        });
    }

    private void executeWithSemaphore(Runnable operation) {

        boolean acquired = false;
        try {
            acquired = semaphore.tryAcquire(SEMAPHORE_WAIT_TIME, TimeUnit.SECONDS);

            operation.run();
        }
        catch (InterruptedException e) {
            LOGGER.error("Not able to acquire semaphore after {}s", SEMAPHORE_WAIT_TIME);
            throw new DebeziumException("Not able to acquire semaphore during signaling processing", e);
        }
        finally {
            if (acquired) {
                semaphore.release();
            }
        }
    }

    private void processSignal(SignalRecord signalRecord, P knownPartition) {

        LOGGER.debug("Signal Processor partition offsets: {}", partitionOffsets.keySet());
        LOGGER.debug("Received signal id = '{}', type = '{}', data = '{}'", signalRecord.getId(), signalRecord.getType(), signalRecord.getData());
        final SignalAction<P> action = signalActions.get(signalRecord.getType());
        if (action == null) {
            LOGGER.warn("Signal '{}' has been received but the type '{}' is not recognized", signalRecord.getId(), signalRecord.getType());
            return;
        }
        try {
            final Document jsonData = (signalRecord.getData() == null || signalRecord.getData().isEmpty()) ? Document.create()
                    : documentReader.read(signalRecord.getData());

            if (knownPartition != null) {
                // Source signal: We know the partition from CDC event
                // TODO the problem here is that since we have just one signal data collection
                // we use then as partition and offsets the one related to the database where the signal data collection reside.
                // This lead to events emitted with a wrong partition/offset.
                // An idea could be to add the id used in the execute-snapshot signal during while emitting open/close signal as a metadata.
                // In this case if we maintain a map of id and related partition we could identify the right one.
                O offset = partitionOffsets.get(knownPartition);
                if (offset != null) {
                    LOGGER.info("Processing source signal '{}' for partition {}", signalRecord.getId(), knownPartition);
                    executeSignalForPartition(action, signalRecord, jsonData, knownPartition, offset);
                }
                else {
                    LOGGER.warn("No offset context found for partition {}", knownPartition);
                }
            }
            else {
                // External signal (Kafka/JMX/File)
                if (partitionOffsets.size() == 1) {
                    // Optimization: If only one partition, use it directly without extraction
                    Map.Entry<P, O> entry = partitionOffsets.entrySet().iterator().next();
                    LOGGER.info("Processing signal '{}' for single partition: {}", signalRecord.getId(), entry.getKey());
                    executeSignalForPartition(action, signalRecord, jsonData, entry.getKey(), entry.getValue());
                }
                else {
                    // Multiple partitions: Extract partition from signal data
                    P targetPartition = extractPartitionFromData(jsonData);

                    if (targetPartition != null) {
                        O offset = partitionOffsets.get(targetPartition);
                        if (offset != null) {
                            LOGGER.info("Processing signal '{}' for extracted partition: {}", signalRecord.getId(), targetPartition);
                            executeSignalForPartition(action, signalRecord, jsonData, targetPartition, offset);
                        }
                        else {
                            LOGGER.warn("Signal references partition {} which is not managed by this task. " +
                                    "Available partitions: {}", targetPartition, partitionOffsets.keySet());
                        }
                    }
                    else {
                        // No partition extracted - broadcast to all partitions (for signals without data-collections)
                        LOGGER.info("Processing signal '{}' for all {} partition(s) (no data-collections specified)",
                                signalRecord.getId(), partitionOffsets.size());
                        for (Map.Entry<P, O> entry : partitionOffsets.entrySet()) {
                            executeSignalForPartition(action, signalRecord, jsonData, entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            LOGGER.warn("Signal '{}' has been received but the data '{}' cannot be parsed", signalRecord.getId(), signalRecord.getData(), e);
        }
        catch (InterruptedException e) {
            LOGGER.warn("Action {} has been interrupted. The signal {} may not have been processed.", signalRecord.getType(), signalRecord);
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            LOGGER.warn("Action {} failed. The signal {} may not have been processed.", signalRecord.getType(), signalRecord, e);
        }
    }

    private P extractPartitionFromData(Document jsonData) {
        final Array dataCollectionsArray = jsonData.getArray("data-collections");
        if (dataCollectionsArray == null || dataCollectionsArray.isEmpty()) {
            LOGGER.debug("No data-collections found in signal data");
            return null;
        }

        // Extract database name from first data collection
        for (Array.Entry entry : dataCollectionsArray) {
            String dataCollection = entry.getValue().asString().trim();

            String[] parts = dataCollection.split("\\.");
            if (parts.length >= 2) {
                String databaseName = parts[0];

                for (P partition : partitionOffsets.keySet()) {
                    if (matchesDatabase(partition, databaseName)) {
                        LOGGER.debug("Matched data collection '{}' to partition {}", dataCollection, partition);
                        return partition;
                    }
                }
            }
            else {
                LOGGER.warn("Data collection '{}' does not contain database qualifier", dataCollection);
            }
        }

        return null;
    }

    private boolean matchesDatabase(P partition, String databaseName) {
        if (partition instanceof AbstractPartition) {
            Map<String, String> loggingContext = partition.getLoggingContext();
            String partitionDatabaseName = loggingContext.get("database.name");
            return partitionDatabaseName != null && databaseName.equalsIgnoreCase(partitionDatabaseName);
        }
        return false;
    }

    private void executeSignalForPartition(SignalAction<P> action, SignalRecord signalRecord,
                                           Document jsonData, P partition, O offset)
            throws InterruptedException {
        SignalPayload<P> payload = new SignalPayload<>(
                partition,
                signalRecord.getId(),
                signalRecord.getType(),
                jsonData,
                offset,
                signalRecord.getAdditionalData());
        action.arrived(payload);
    }

    /**
     * The method permits to get specified SignalChannelReader instance from the available SPI implementations
     * @param channel the class of the channel to get
     * @return the specified instance from the available SPI implementations
     */
    public <T extends SignalChannelReader> T getSignalChannel(Class<T> channel) {
        return channel.cast(signalChannelReaders.stream()
                .filter(isSignal(channel))
                .findFirst().get());
    }

    private static <T extends SignalChannelReader> Predicate<SignalChannelReader> isSignal(Class<T> channelClass) {
        return channel -> channel.getClass().equals(channelClass);
    }
}

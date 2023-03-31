/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.channels.SignalChannelReader;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Threads;

/**
 * This class permits to process signals coming from the different channels.
 *
 * @author Mario Fiore Vitale
 */
public class SignalProcessor<P extends Partition, O extends OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalProcessor.class);

    /**
     * Waiting period for the polling loop to finish. Will be applied twice, once gracefully, once forcefully.
     */
    public static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.ofSeconds(90);

    private final Map<String, SignalAction<P>> signalActions = new HashMap<>();

    private final CommonConnectorConfig connectorConfig;

    private final List<SignalChannelReader> signalChannelReaders;

    private final ScheduledExecutorService signalProcessorExecutor;

    private final DocumentReader documentReader;

    private Offsets<P, O> previousOffsets;

    public SignalProcessor(Class<? extends SourceConnector> connector,
                           CommonConnectorConfig config,
                           Map<String, SignalAction<P>> signalActions,
                           List<SignalChannelReader> signalChannelReaders, DocumentReader documentReader,
                           Offsets<P, O> previousOffsets) {

        this.connectorConfig = config;
        this.signalChannelReaders = signalChannelReaders;
        this.documentReader = documentReader;
        this.previousOffsets = previousOffsets;
        this.signalProcessorExecutor = Threads.newSingleThreadScheduledExecutor(connector, config.getLogicalName(), SignalProcessor.class.getSimpleName(), false);

        signalChannelReaders.stream()
                .filter(isEnabled())
                .forEach(signalChannelReader -> signalChannelReader.init(connectorConfig));

        this.signalActions.putAll(signalActions);
    }

    private Predicate<SignalChannelReader> isEnabled() {
        return reader -> connectorConfig.getEnabledChannels().contains(reader.name());
    }

    public void setContext(O offset) {
        previousOffsets = Offsets.of(Collections.singletonMap(previousOffsets.getTheOnlyPartition(), offset));
    }

    public void start() {

        LOGGER.info("SignalProcessor started. Scheduling it every {}ms", connectorConfig.getSignalPollInterval().toMillis());
        signalProcessorExecutor.scheduleAtFixedRate(this::process, 0, 1, TimeUnit.NANOSECONDS);
    }

    public void stop() throws InterruptedException {

        // The close must run with same thread of the read otherwise Kafka client will detect multi-thread and throw and exception
        signalProcessorExecutor.submit(() -> signalChannelReaders.stream()
                .filter(isEnabled())
                .forEach(SignalChannelReader::close));

        signalProcessorExecutor.shutdown();
        boolean isShutdown = signalProcessorExecutor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        if (!isShutdown) {
            LOGGER.warn("SignalProcessor didn't stop in the expected time, shutting down executor now");

            // Clear interrupt flag so the forced termination is always attempted
            Thread.interrupted();
            signalProcessorExecutor.shutdownNow();
            signalProcessorExecutor.awaitTermination(SHUTDOWN_WAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        LOGGER.info("SignalProcessor stopped");
    }

    public void registerSignalAction(String id, SignalAction<P> signal) {

        LOGGER.debug("Registering signal '{}' using class '{}'", id, signal.getClass().getName());
        signalActions.put(id, signal);
    }

    public synchronized void process() {

        LOGGER.trace("SignalProcessor processing");
        signalChannelReaders.parallelStream()
                .filter(isEnabled())
                .map(SignalChannelReader::read)
                .flatMap(Collection::stream)
                .forEach(this::processSignal);
    }

    private void processSignal(SignalRecord signalRecord) {

        LOGGER.debug("Signal Processor offset context {}", previousOffsets.getOffsets());
        LOGGER.debug("Received signal id = '{}', type = '{}', data = '{}'", signalRecord.getId(), signalRecord.getType(), signalRecord.getData());
        final SignalAction<P> action = signalActions.get(signalRecord.getType());
        if (action == null) {
            LOGGER.warn("Signal '{}' has been received but the type '{}' is not recognized", signalRecord.getId(), signalRecord.getType());
            return;
        }
        try {
            final Document jsonData = (signalRecord.getData() == null || signalRecord.getData().isEmpty()) ? Document.create()
                    : documentReader.read(signalRecord.getData());

            action.arrived(new SignalPayload<>(previousOffsets.getTheOnlyPartition(), signalRecord.getId(), signalRecord.getType(), jsonData,
                    previousOffsets.getTheOnlyOffset(), signalRecord.getAdditionalData()));
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

    public SourceSignalChannel getSourceSignalChannel() {
        return (SourceSignalChannel) signalChannelReaders.stream()
                .filter(channel -> channel.name().equals(SourceSignalChannel.CHANNEL_NAME))
                .findFirst().get();
    }
}

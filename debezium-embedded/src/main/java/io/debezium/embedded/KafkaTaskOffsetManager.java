/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Instantiator;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;

public class KafkaTaskOffsetManager implements TaskOffsetManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTaskOffsetManager.class);

    private final Clock clock;
    private final SourceTask sourceTask;
    private final EmbeddedEngineState embeddedEngineState;
    private final Duration commitTimeout;
    private final OffsetCommitPolicy offsetCommitPolicy;
    private final OffsetStorageWriter offsetStorageWriter;
    private final OffsetStorageReader offsetStorageReader;
    private long recordsSinceLastCommit = 0;
    private long timeOfLastCommitMillis;

    public KafkaTaskOffsetManager(
                                  OffsetManager offsetManager,
                                  Clock clock,
                                  SourceTask sourceTask,
                                  EmbeddedEngineState embeddedEngineState,
                                  Configuration config) {
        this.clock = clock;
        this.sourceTask = sourceTask;
        this.embeddedEngineState = embeddedEngineState;

        this.timeOfLastCommitMillis = clock.currentTimeInMillis();

        // Set up the offset commit policy ...
        this.offsetCommitPolicy = Instantiator.getInstanceWithProperties(
                config.getString(OFFSET_COMMIT_POLICY), config.asProperties());
        this.commitTimeout = Duration.ofMillis(config.getLong(OFFSET_COMMIT_TIMEOUT_MS));

        final String engineName = config.getString(EmbeddedEngine.ENGINE_NAME);

        Map<String, String> internalConverterConfig = Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");

        Converter keyConverter = Instantiator.getInstance(JsonConverter.class.getName());
        keyConverter.configure(internalConverterConfig, true);

        Converter valueConverter = Instantiator.getInstance(JsonConverter.class.getName());
        valueConverter.configure(internalConverterConfig, false);

        this.offsetStorageWriter = new OffsetStorageWriter(offsetManager.getOffsetBackingStore(), engineName, keyConverter, valueConverter);
        this.offsetStorageReader = new OffsetStorageReaderImpl(offsetManager.getOffsetBackingStore(), engineName, keyConverter, valueConverter);
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
        return this.offsetStorageReader;
    }

    @Override
    public void commit(SourceRecord record) throws InterruptedException {
        sourceTask.commitRecord(record);
        offset(record.sourcePartition(), record.sourceOffset());
    }

    public void offset(Map<String, ?> partition, Map<String, ?> offset) {
        ++recordsSinceLastCommit;
        this.offsetStorageWriter.offset(partition, offset);
    }

    /**
     * Determine if we should flush offsets to storage, and if so then attempt to flush offsets.
     */
    public void maybeFlush() throws InterruptedException {
        // Determine if we need to commit to offset storage ...
        long timeSinceLastCommitMillis = clock.currentTimeInMillis() - timeOfLastCommitMillis;
        if (offsetCommitPolicy.performCommit(recordsSinceLastCommit, Duration.ofMillis(timeSinceLastCommitMillis))) {
            commitOffsets();
        }
    }

    /**
     * Flush offsets to storage.
     */
    public void commitOffsets() throws InterruptedException {
        long started = clock.currentTimeInMillis();
        long timeout = started + commitTimeout.toMillis();
        if (!offsetStorageWriter.beginFlush()) {
            return;
        }
        Future<Void> flush = offsetStorageWriter.doFlush(this::completedFlush);
        if (flush == null) {
            return; // no offsets to commit ...
        }

        // Wait until the offsets are flushed ...
        try {
            flush.get(Math.max(timeout - clock.currentTimeInMillis(), 0), TimeUnit.MILLISECONDS);
            // if we've gotten this far, the offsets have been committed so notify the task
            sourceTask.commit();
            recordsSinceLastCommit = 0;
            timeOfLastCommitMillis = clock.currentTimeInMillis();
        }
        catch (InterruptedException e) {
            LOGGER.warn("Flush of {} offsets interrupted, cancelling", this);
            offsetStorageWriter.cancelFlush();

            if (!embeddedEngineState.isStopped()) {
                // engine is still running -> we were not interrupted
                // due the stop() call -> probably someone else called the interrupt on us ->
                // -> we should raise the interrupt flag
                Thread.currentThread().interrupt();
                throw e;
            }
        }
        catch (ExecutionException e) {
            LOGGER.error("Flush of {} offsets threw an unexpected exception: ", this, e);
            offsetStorageWriter.cancelFlush();
        }
        catch (TimeoutException e) {
            LOGGER.error("Timed out waiting to flush {} offsets to storage", this);
            offsetStorageWriter.cancelFlush();
        }
    }

    protected void completedFlush(Throwable error, Void result) {
        if (error != null) {
            LOGGER.error("Failed to flush {} offsets to storage: ", this, error);
        }
        else {
            LOGGER.trace("Finished flushing {} offsets to storage", this);
        }
    }
}

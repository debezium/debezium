/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.nats.NatsConnection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

/**
 * A {@link SchemaHistory} implementation that records schema changes as
 * messages in a NATS JetStream stream,
 * and recovers the history by consuming all messages from that stream.
 *
 * @author Nick Chomey
 */
@NotThreadSafe
public class NatsSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(NatsSchemaHistory.class);

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    private NatsSchemaHistoryConfig config;
    private NatsConnection natsConnection;
    private JetStream jetStream;
    private JetStreamManagement jetStreamManagement;
    private ExecutorService executor;
    private String dbHistoryName;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener,
                          boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.config = new NatsSchemaHistoryConfig(config);
        this.dbHistoryName = config.getString(SchemaHistory.NAME, UUID.randomUUID().toString());
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nats-schema-history-" + dbHistoryName);
            t.setDaemon(true);
            return t;
        });

        LOGGER.info("Configured NATS schema history with stream '{}' and subject '{}'",
                this.config.getStreamName(), this.config.getSubject());
    }

    @Override
    public void start() {
        super.start();
        try {
            natsConnection = NatsConnection.getInstance(config, config.instanceScope());
            jetStream = natsConnection.getJetStream();
            jetStreamManagement = natsConnection.getJetStreamManagement();

            LOGGER.info("Started NATS schema history");
        }
        catch (Exception e) {
            throw new SchemaHistoryException("Failed to start NATS schema history", e);
        }
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (jetStream == null) {
            throw new SchemaHistoryException(
                    "No NATS JetStream available. Ensure that 'start()' is called before storing schema history records.");
        }

        LOGGER.trace("Storing record into NATS schema history: {}", record);
        try {
            String recordString = writer.write(record.document());
            jetStream.publish(config.getSubject(), recordString.getBytes());
            LOGGER.debug("Stored schema history record in subject '{}'", config.getSubject());
        }
        catch (Exception e) {
            throw new SchemaHistoryException("Failed to store schema history record", e);
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (executor != null) {
            executor.shutdown();
        }
        if (natsConnection != null) {
            natsConnection.close();
        }
        LOGGER.info("Stopped NATS schema history");
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) throws InterruptedException {
        try {
            LOGGER.debug("Recovering schema history from NATS stream '{}'", config.getStreamName());

            // Create a pull consumer to read all messages from the beginning
            String consumerName = "schema-history-recovery-" + UUID.randomUUID().toString();
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable(consumerName)
                    .deliverPolicy(DeliverPolicy.All)
                    .build();

            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .configuration(consumerConfig)
                    .build();

            JetStreamSubscription subscription = jetStream.subscribe(config.getSubject(), pullOptions);

            int recoveryAttempts = 0;
            int maxRecoveryAttempts = config.getRecoveryAttempts();
            long pollInterval = config.getRecoveryPollIntervalMs();

            while (recoveryAttempts < maxRecoveryAttempts) {
                checkForInterruption();

                // Fetch messages in batches
                subscription.fetch(100, Duration.ofMillis(pollInterval))
                        .forEach(message -> {
                            try {
                                checkForInterruption();
                                String recordString = new String(message.getData());
                                if (recordString != null && !recordString.trim().isEmpty()) {
                                    HistoryRecord record = new HistoryRecord(reader.read(recordString));
                                    LOGGER.trace("Recovered schema history record: {}", record);
                                    records.accept(record);
                                }
                                message.ack();
                            }
                            catch (Exception e) {
                                LOGGER.warn("Failed to process schema history record", e);
                            }
                        });

                recoveryAttempts++;

                // Check if we've reached the end of the stream
                if (subscription.getConsumerInfo().getNumPending() == 0) {
                    LOGGER.debug("Reached end of schema history stream after {} attempts", recoveryAttempts);
                    break;
                }
            }

            subscription.unsubscribe();
            LOGGER.info("Schema history recovery completed");

        }
        catch (Exception e) {
            throw new SchemaHistoryException("Failed to recover schema history from NATS", e);
        }
    }

    @Override
    public boolean storageExists() {
        try {
            jetStreamManagement.getStreamInfo(config.getStreamName());
            LOGGER.info("NATS stream '{}' used to store schema history exists", config.getStreamName());
            return true;
        }
        catch (Exception e) {
            LOGGER.info("NATS stream '{}' used to store schema history does not exist yet", config.getStreamName());
            return false;
        }
    }

    @Override
    public boolean exists() {
        try {
            if (jetStreamManagement == null) {
                return false;
            }

            // Check if the stream exists and has messages
            StreamInfo streamInfo = jetStreamManagement.getStreamInfo(config.getStreamName());
            return streamInfo != null && streamInfo.getStreamState().getMsgCount() > 0;
        }
        catch (Exception e) {
            LOGGER.debug("Error checking if schema history exists", e);
            return false;
        }
    }

    @Override
    public void initializeStorage() {
        try {
            // Ensure connection is established before initializing storage
            if (natsConnection == null) {
                natsConnection = NatsConnection.getInstance(config, config.instanceScope());
                jetStream = natsConnection.getJetStream();
                jetStreamManagement = natsConnection.getJetStreamManagement();
            }

            LOGGER.info("Creating NATS stream '{}' for schema history storage", config.getStreamName());

            StorageType storageType = "memory".equals(config.getStorageType()) ? StorageType.Memory : StorageType.File;

            StreamConfiguration.Builder streamBuilder = StreamConfiguration.builder()
                    .name(config.getStreamName())
                    .subjects(config.getSubject())
                    .storageType(storageType)
                    .replicas(config.getReplicas());

            if (config.getMaxAgeMs() > 0) {
                streamBuilder.maxAge(Duration.ofMillis(config.getMaxAgeMs()));
            }

            if (config.getMaxBytes() > 0) {
                streamBuilder.maxBytes(config.getMaxBytes());
            }

            jetStreamManagement.addStream(streamBuilder.build());
            LOGGER.info("Successfully created NATS stream '{}'", config.getStreamName());

        }
        catch (Exception e) {
            throw new SchemaHistoryException("Failed to initialize NATS stream for schema history", e);
        }
    }

    @Override
    public String toString() {
        return "NATS JetStream";
    }

    private void checkForInterruption() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Schema history recovery was interrupted");
        }
    }
}

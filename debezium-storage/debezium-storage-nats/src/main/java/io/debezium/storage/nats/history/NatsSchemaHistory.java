/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats.history;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.annotation.VisibleForTesting;
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
import io.debezium.util.DelayStrategy;
import io.debezium.util.RetryingRunnable;
import io.debezium.util.Strings;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.support.NatsJetStreamConstants;

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

    /**
     * JetStream API error reported when the stream does not exist, named
     * {@code JSStreamNotFoundErr} in the NATS server error registry.
     * <p>
     * The value is hard-coded because the NATS Java client does not expose it:
     * {@code NatsJetStreamConstants} carries only a handful of other JetStream
     * error codes.
     */
    private static final int STREAM_NOT_FOUND_API_ERROR_CODE = 10059;

    /**
     * JetStream API error reported when a stream with the requested name exists
     * with a different configuration, named {@code JSStreamNameExistErr} in the
     * server error registry. Not exposed by the client library either.
     */
    private static final int STREAM_NAME_EXIST_API_ERROR_CODE = 10058;

    /**
     * Delay between retries of a schema history publish. A publish normally
     * fails for a transient reason, such as a timeout, a reconnect or a
     * server-side handover.
     */
    private static final Duration PUBLISH_RETRY_DELAY = Duration.ofMillis(100);

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    private NatsSchemaHistoryConfig config;
    private NatsConnection natsConnection;
    private JetStream jetStream;
    private JetStreamManagement jetStreamManagement;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener,
                          boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.config = new NatsSchemaHistoryConfig(config);

        LOGGER.info("Configured NATS schema history with stream '{}' and subject '{}'",
                this.config.getStreamName(), this.config.getSubject());
    }

    @Override
    public void start() {
        super.start();
        try {
            connect();

            LOGGER.info("Started NATS schema history");
        }
        catch (Exception e) {
            throw new SchemaHistoryException("Failed to start NATS schema history", e);
        }
    }

    /**
     * Establish the NATS connection and the JetStream handles, reusing the
     * connection when one already exists (for example when
     * {@code initializeStorage()} runs before {@code start()}). A single
     * {@link NatsSchemaHistory} owns exactly one connection.
     */
    private void connect() throws IOException, InterruptedException {
        if (natsConnection == null) {
            natsConnection = new NatsConnection(config);
        }
        jetStream = natsConnection.getJetStream();
        jetStreamManagement = natsConnection.getJetStreamManagement();
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
            byte[] payload = recordString.getBytes(StandardCharsets.UTF_8);

            // A publish that reaches the server but loses its acknowledgement would be
            // written a second time by the retry below. Reusing one message ID across
            // the attempts lets the stream discard that duplicate: JetStream remembers
            // message IDs for its duplicate window, which defaults to two minutes and
            // comfortably covers the retries here.
            Headers headers = new Headers().add(NatsJetStreamConstants.MSG_ID_HDR, UUID.randomUUID().toString());

            // A publish failure is usually transient, so network errors are
            // retried. Whether it was instead caused by the stream disappearing
            // is decided below: recreating the stream would silently continue
            // with a history that has lost every record stored before it.
            RetryingRunnable.<Exception> builder()
                    .retries(natsConnection.getRetryBudget())
                    .delayStrategy(DelayStrategy.constant(PUBLISH_RETRY_DELAY))
                    .retriableExceptions(IOException.class)
                    .doRun(() -> jetStream.publish(config.getSubject(), headers, payload))
                    .build()
                    .run();

            LOGGER.debug("Stored schema history record in subject '{}'", config.getSubject());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SchemaHistoryException("Interrupted while storing schema history record", e);
        }
        catch (Exception e) {
            // A publish failure is usually transient, and the retries above have
            // already been exhausted. If the stream is genuinely gone, however,
            // every DDL recorded before it is gone too, and recreating the stream
            // would hide that. Fail with something the user can act on instead.
            if (!streamExistsQuietly()) {
                throw new SchemaHistoryException(String.format(
                        "NATS stream '%s' no longer exists, so the database schema history cannot be recorded. "
                                + "Any DDL that it held has been lost. Restart the connector with a "
                                + "'recovery' snapshot to rebuild the schema history.",
                        config.getStreamName()), e);
            }
            throw new SchemaHistoryException("Failed to store schema history record", e);
        }
    }

    /**
     * Best-effort check of whether the schema history stream still exists.
     * Returns {@code true} when the check itself fails, so that an unrelated
     * problem is never reported as a lost stream.
     */
    private boolean streamExistsQuietly() {
        try {
            return storageExists();
        }
        catch (Exception e) {
            LOGGER.debug("Failed to determine whether NATS stream '{}' still exists", config.getStreamName(), e);
            return true;
        }
    }

    @Override
    public void stop() {
        super.stop();
        if (natsConnection != null) {
            natsConnection.close();
        }
        LOGGER.info("Stopped NATS schema history");
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) throws InterruptedException {
        try {
            LOGGER.debug("Recovering schema history from NATS stream '{}'", config.getStreamName());

            // Create an ephemeral pull consumer to read all messages from the
            // beginning. Ephemeral (non-durable) is intentional: a durable
            // consumer with a random name would leak consumer state in the
            // stream on every recovery without ever being resumed.
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .deliverPolicy(DeliverPolicy.All)
                    .build();

            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .configuration(consumerConfig)
                    .build();

            JetStreamSubscription subscription = jetStream.subscribe(config.getSubject(), pullOptions);
            try {
                int recoveryAttempts = 0;
                long pollInterval = config.getRecoveryPollIntervalMs();
                long deadline = System.currentTimeMillis() + config.getRecoveryTimeoutMs();

                while (System.currentTimeMillis() < deadline) {
                    checkForInterruption();

                    // Fetch messages in batches
                    var messages = subscription.fetch(100, Duration.ofMillis(pollInterval));
                    for (Message message : messages) {
                        checkForInterruption();
                        try {
                            String recordString = new String(message.getData(), StandardCharsets.UTF_8);
                            if (!Strings.isNullOrBlank(recordString)) {
                                HistoryRecord record = new HistoryRecord(reader.read(recordString));
                                if (record.isValid()) {
                                    LOGGER.trace("Recovered schema history record: {}", record);
                                    records.accept(record);
                                }
                                else {
                                    LOGGER.warn("Skipping invalid schema history record '{}' from subject '{}'", record,
                                            config.getSubject());
                                }
                            }
                        }
                        catch (IOException e) {
                            // Only a record that cannot be deserialized is skipped. Anything else,
                            // including a failure to apply the record itself, has to fail recovery:
                            // continuing would rebuild an incomplete schema, and the connector would
                            // then emit events describing the wrong table structure.
                            LOGGER.warn("Skipping schema history record from subject '{}' that could not be deserialized",
                                    config.getSubject(), e);
                        }
                        finally {
                            // Acknowledge every fetched message, including a skipped one, so the batch
                            // makes progress exactly as a fully processed batch would.
                            message.ack();
                        }
                    }

                    recoveryAttempts++;

                    // Check if we've reached the end of the stream
                    if (subscription.getConsumerInfo().getNumPending() == 0) {
                        LOGGER.debug("Reached end of schema history stream after {} attempts", recoveryAttempts);
                        break;
                    }
                }

                long numPending = subscription.getConsumerInfo().getNumPending();
                if (numPending > 0) {
                    // Continuing with a partial model would emit events describing
                    // the wrong table structure, so recovery has to fail the way a
                    // missing Kafka history topic does.
                    throw new SchemaHistoryException(String.format(
                            "The database schema history couldn't be recovered: %d messages were still pending "
                                    + "after %d ms. Consider increasing '%s'.",
                            numPending, config.getRecoveryTimeoutMs(),
                            NatsSchemaHistoryConfig.PROP_RECOVERY_TIMEOUT_MS.name()));
                }

                LOGGER.info("Schema history recovery completed");
            }
            finally {
                // Release the ephemeral consumer on every path, including failure.
                try {
                    subscription.unsubscribe();
                }
                catch (Exception e) {
                    LOGGER.debug("Failed to unsubscribe the schema history recovery consumer", e);
                }
            }
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (SchemaHistoryException e) {
            throw e;
        }
        catch (Exception e) {
            throw new SchemaHistoryException("Failed to recover schema history from NATS", e);
        }
    }

    @Override
    public boolean storageExists() {
        StreamInfo streamInfo = streamOrNull();
        LOGGER.info("NATS stream '{}' used to store schema history {}", config.getStreamName(),
                streamInfo == null ? "does not exist yet" : "exists");
        return streamInfo != null;
    }

    @Override
    public boolean exists() {
        // The stream can exist while holding no records yet, which is why the
        // interface asks the two questions separately.
        StreamInfo streamInfo = streamOrNull();
        if (streamInfo == null) {
            return false;
        }
        // BaseSourceTask only calls checkStorageSettings() when a connector starts with
        // no previous offset, so a restart would otherwise never hear about the stream
        // settings. The metadata is already in hand, so reporting them costs nothing.
        logStreamSettings(streamInfo.getConfiguration());
        return streamInfo.getStreamState().getMsgCount() > 0;
    }

    /**
     * Fetch the metadata of the schema history stream, or {@code null} when the
     * server says the stream does not exist.
     * <p>
     * Being unable to ask is not the same as the stream being absent. Callers use
     * this answer to decide whether to initialize new storage or to warn that an
     * intact history is missing, so any other failure has to propagate.
     */
    private StreamInfo streamOrNull() {
        if (jetStreamManagement == null) {
            throw new SchemaHistoryException(
                    "No NATS JetStream available. Ensure that 'start()' is called before checking the schema history storage.");
        }

        try {
            return jetStreamManagement.getStreamInfo(config.getStreamName());
        }
        catch (Exception e) {
            // The one answer here that is not a failure is the server reporting
            // that there simply is no such stream.
            if (e instanceof JetStreamApiException apiError
                    && apiError.getApiErrorCode() == STREAM_NOT_FOUND_API_ERROR_CODE) {
                return null;
            }
            throw couldNotCheckStream(e);
        }
    }

    /**
     * Builds the failure to report when the stream metadata could not be read.
     */
    private SchemaHistoryException couldNotCheckStream(Exception e) {
        return new SchemaHistoryException(
                String.format("Failed to check the schema history NATS stream '%s'", config.getStreamName()), e);
    }

    @Override
    public void checkStorageSettings() {
        try {
            StreamInfo streamInfo = streamOrNull();
            if (streamInfo != null) {
                logStreamSettings(streamInfo.getConfiguration());
            }
        }
        catch (Exception e) {
            // This check only advises the user, so it must not stop the connector.
            LOGGER.warn("Failed to check the settings of stream '{}'", config.getStreamName(), e);
        }
    }

    /**
     * Logs everything about the stream that the user should know about but that does
     * not stop the connector from running.
     */
    private void logStreamSettings(StreamConfiguration streamConfiguration) {
        retentionWarning(streamConfiguration).ifPresent(LOGGER::warn);
        deduplicationWarning(streamConfiguration, publishRetryWindow()).ifPresent(LOGGER::warn);
    }

    /**
     * How long a single publish can spend being retried. The stream's duplicate
     * window has to cover at least this long for a retried record to be recognized
     * as a duplicate rather than stored again.
     */
    private Duration publishRetryWindow() {
        return PUBLISH_RETRY_DELAY.multipliedBy(natsConnection.getRetryBudget());
    }

    /**
     * Describes how the retention settings of the stream can cause only part of the
     * schema history to be recovered, or empty when the stream retains all of it.
     * <p>
     * A stream that discards its oldest messages will lose the beginning of the
     * history, and a later recovery will silently rebuild a schema from whatever is
     * left. That is not necessarily wrong, so this is a warning rather than a
     * failure, but the user should hear about it before it happens.
     */
    @VisibleForTesting
    static Optional<String> retentionWarning(StreamConfiguration streamConfiguration) {
        List<String> limits = new ArrayList<>();
        Duration maxAge = streamConfiguration.getMaxAge();
        if (maxAge != null && !maxAge.isZero() && !maxAge.isNegative()) {
            limits.add(String.format("'%s' is %s", NatsSchemaHistoryConfig.PROP_MAX_AGE_MS.name(), maxAge));
        }
        if (streamConfiguration.getMaxBytes() > 0) {
            limits.add(String.format("'%s' is %d bytes", NatsSchemaHistoryConfig.PROP_MAX_BYTES.name(),
                    streamConfiguration.getMaxBytes()));
        }
        if (limits.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.format(
                "NATS stream '%s' does not retain its schema history indefinitely (%s). Older schema history records "
                        + "will be discarded, and a later recovery will rebuild an incomplete schema. Remove the limit, "
                        + "or raise it enough to hold the entire history.",
                streamConfiguration.getName(), String.join(" and ", limits)));
    }

    /**
     * Describes how the stream's duplicate window can fail to deduplicate a retried
     * publish, or empty when it covers the retry.
     * <p>
     * A window shorter than the time a publish can spend being retried allows a record
     * whose acknowledgement was lost to be stored a second time, so recovery replays a
     * DDL statement. A zero or negative window is left alone: the server substitutes
     * its own default for those rather than disabling deduplication.
     */
    @VisibleForTesting
    static Optional<String> deduplicationWarning(StreamConfiguration streamConfiguration, Duration publishRetryWindow) {
        Duration window = streamConfiguration.getDuplicateWindow();
        if (window == null || window.isZero() || window.isNegative()) {
            // Either the server substituted its own default or the stream reports no
            // window at all, so there is no explicit value to question.
            return Optional.empty();
        }
        if (window.compareTo(publishRetryWindow) < 0) {
            String property = NatsSchemaHistoryConfig.PROP_DUPLICATE_WINDOW_MS.name();
            return Optional.of(String.format(
                    "NATS stream '%s' has a message ID duplicate window of %s ('%s'), which is shorter than the %s a "
                            + "publish can spend being retried, so a retry after a lost acknowledgement may be stored "
                            + "twice. Raise '%s'.",
                    streamConfiguration.getName(), window, property, publishRetryWindow, property));
        }
        return Optional.empty();
    }

    @Override
    public void initializeStorage() {
        try {
            // Ensure connection is established before initializing storage
            connect();

            LOGGER.info("Creating NATS stream '{}' for schema history storage", config.getStreamName());

            StorageType storageType = config.getStorageType() == NatsSchemaHistoryConfig.StorageType.MEMORY
                    ? StorageType.Memory
                    : StorageType.File;

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

            if (config.getDuplicateWindowMs() > 0) {
                // Only set explicitly when asked: a zero or negative value leaves the
                // server default in place, and the server substitutes that default for
                // an explicit zero anyway.
                streamBuilder.duplicateWindow(Duration.ofMillis(config.getDuplicateWindowMs()));
            }

            try {
                jetStreamManagement.addStream(streamBuilder.build());
                LOGGER.info("Successfully created NATS stream '{}'", config.getStreamName());
            }
            catch (JetStreamApiException e) {
                if (e.getApiErrorCode() == STREAM_NAME_EXIST_API_ERROR_CODE) {
                    // Stream already exists, possibly with a different
                    // configuration (e.g. after a config change or a racing
                    // connector). Reuse it rather than failing hard.
                    LOGGER.warn("NATS stream '{}' already exists with a different configuration; "
                            + "reusing the existing stream", config.getStreamName());
                }
                else {
                    throw e;
                }
            }

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

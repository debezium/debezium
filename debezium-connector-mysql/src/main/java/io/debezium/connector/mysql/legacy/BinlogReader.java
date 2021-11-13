/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.legacy;

import static io.debezium.util.Strings.isNullOrEmpty;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.BitSet;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.event.Level;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import com.github.shyiko.mysql.binlog.network.DefaultSSLSocketFactory;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.SSLSocketFactory;

import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.EventDataDeserializationExceptionData;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.HaltingPredicate;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.RowDeserializers;
import io.debezium.connector.mysql.StopEventDataDeserializer;
import io.debezium.connector.mysql.legacy.RecordMakers.RecordsForTable;
import io.debezium.data.Envelope.Operation;
import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * A component that reads the binlog of a MySQL server, and records any schema changes in {@link MySqlSchema}.
 *
 * @author Randall Hauch
 *
 */
public class BinlogReader extends AbstractReader {

    private static final long INITIAL_POLL_PERIOD_IN_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private static final long MAX_POLL_PERIOD_IN_MILLIS = TimeUnit.HOURS.toMillis(1);
    private static final String KEEPALIVE_THREAD_NAME = "blc-keepalive";

    private final boolean recordSchemaChangesInSourceRecords;
    private final RecordMakers recordMakers;
    private final SourceInfo source;
    private final EnumMap<EventType, BlockingConsumer<Event>> eventHandlers = new EnumMap<>(EventType.class);
    private final BinaryLogClient client;
    private final BinlogReaderMetrics metrics;
    private final Clock clock;
    private final ElapsedTimeStrategy pollOutputDelay;
    private final EventProcessingFailureHandlingMode eventDeserializationFailureHandlingMode;
    private final EventProcessingFailureHandlingMode inconsistentSchemaHandlingMode;

    private int startingRowNumber = 0;
    private long recordCounter = 0L;
    private long previousOutputMillis = 0L;
    private long initialEventsToSkip = 0L;
    private boolean skipEvent = false;
    private boolean ignoreDmlEventByGtidSource = false;
    private final Predicate<String> gtidDmlSourceFilter;
    private final AtomicLong totalRecordCounter = new AtomicLong();
    private volatile Map<String, ?> lastOffset = null;
    private com.github.shyiko.mysql.binlog.GtidSet gtidSet;
    private Heartbeat heartbeat;
    private MySqlJdbcContext connectionContext;
    private final float heartbeatIntervalFactor = 0.8f;
    private final Map<String, Thread> binaryLogClientThreads = new ConcurrentHashMap<>(4);

    public static class BinlogPosition {
        final String filename;
        final long position;

        public BinlogPosition(String filename, long position) {
            assert filename != null;

            this.filename = filename;
            this.position = position;
        }

        public String getFilename() {
            return filename;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public String toString() {
            return filename + "/" + position;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + filename.hashCode();
            result = prime * result + (int) (position ^ (position >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BinlogPosition other = (BinlogPosition) obj;
            if (!filename.equals(other.filename)) {
                return false;
            }
            if (position != other.position) {
                return false;
            }
            return true;
        }
    }

    /**
     * Create a binlog reader.
     *
     * @param name the name of this reader; may not be null
     * @param context the task context in which this reader is running; may not be null
     * @param acceptAndContinue see {@link AbstractReader#AbstractReader(String, MySqlTaskContext, Predicate)}
     */
    public BinlogReader(String name, MySqlTaskContext context, HaltingPredicate acceptAndContinue) {
        this(name, context, acceptAndContinue, context.serverId());
    }

    /**
     * Create a binlog reader.
     *
     * @param name the name of this reader; may not be null
     * @param context the task context in which this reader is running; may not be null
     * @param acceptAndContinue see {@link AbstractReader#AbstractReader(String, MySqlTaskContext, Predicate)}
     * @param serverId the server id to use for the {@link BinaryLogClient}
     */
    public BinlogReader(String name, MySqlTaskContext context, HaltingPredicate acceptAndContinue, long serverId) {
        super(name, context, acceptAndContinue);

        connectionContext = context.getConnectionContext();
        source = context.source();
        recordMakers = context.makeRecord();
        recordSchemaChangesInSourceRecords = context.includeSchemaChangeRecords();
        clock = context.getClock();
        eventDeserializationFailureHandlingMode = connectionContext.eventProcessingFailureHandlingMode();
        inconsistentSchemaHandlingMode = connectionContext.inconsistentSchemaHandlingMode();

        // Use exponential delay to log the progress frequently at first, but the quickly tapering off to once an hour...
        pollOutputDelay = ElapsedTimeStrategy.exponential(clock, INITIAL_POLL_PERIOD_IN_MILLIS, MAX_POLL_PERIOD_IN_MILLIS);

        // Set up the log reader ...
        client = new BinaryLogClient(connectionContext.hostname(), connectionContext.port(), connectionContext.username(), connectionContext.password());
        // BinaryLogClient will overwrite thread names later
        client.setThreadFactory(
                Threads.threadFactory(MySqlConnector.class, context.getConnectorConfig().getLogicalName(), "binlog-client", false, false,
                        x -> binaryLogClientThreads.put(x.getName(), x)));
        client.setServerId(serverId);
        client.setSSLMode(sslModeFor(connectionContext.sslMode()));
        if (connectionContext.sslModeEnabled()) {
            SSLSocketFactory sslSocketFactory = getBinlogSslSocketFactory(connectionContext);
            if (sslSocketFactory != null) {
                client.setSslSocketFactory(sslSocketFactory);
            }
        }
        Configuration configuration = context.config();
        client.setKeepAlive(configuration.getBoolean(MySqlConnectorConfig.KEEP_ALIVE));
        final long keepAliveInterval = configuration.getLong(MySqlConnectorConfig.KEEP_ALIVE_INTERVAL_MS);
        client.setKeepAliveInterval(keepAliveInterval);
        // Considering heartbeatInterval should be less than keepAliveInterval, we use the heartbeatIntervalFactor
        // multiply by keepAliveInterval and set the result value to heartbeatInterval.The default value of heartbeatIntervalFactor
        // is 0.8, and we believe the left time (0.2 * keepAliveInterval) is enough to process the packet received from the MySQL server.
        client.setHeartbeatInterval((long) (keepAliveInterval * heartbeatIntervalFactor));
        client.registerEventListener(context.bufferSizeForBinlogReader() == 0
                ? this::handleEvent
                : (new EventBuffer(context.bufferSizeForBinlogReader(), this))::add);

        client.registerLifecycleListener(new ReaderThreadLifecycleListener());
        client.registerEventListener(this::onEvent);
        if (logger.isDebugEnabled()) {
            client.registerEventListener(this::logEvent);
        }

        boolean filterDmlEventsByGtidSource = configuration.getBoolean(MySqlConnectorConfig.GTID_SOURCE_FILTER_DML_EVENTS);
        gtidDmlSourceFilter = filterDmlEventsByGtidSource ? context.gtidSourceFilter() : null;

        // Set up the event deserializer with additional type(s) ...
        final Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<Long, TableMapEventData>();
        EventDeserializer eventDeserializer = new EventDeserializer() {
            @Override
            public Event nextEvent(ByteArrayInputStream inputStream) throws IOException {
                try {
                    // Delegate to the superclass ...
                    Event event = super.nextEvent(inputStream);

                    // We have to record the most recent TableMapEventData for each table number for our custom deserializers ...
                    if (event.getHeader().getEventType() == EventType.TABLE_MAP) {
                        TableMapEventData tableMapEvent = event.getData();
                        tableMapEventByTableId.put(tableMapEvent.getTableId(), tableMapEvent);
                    }
                    return event;
                }
                // DBZ-217 In case an event couldn't be read we create a pseudo-event for the sake of logging
                catch (EventDataDeserializationException edde) {
                    // DBZ-3233 As of Java 15, when reaching EOF in the binlog stream, the polling loop in
                    // BinaryLogClient#listenForEventPackets() keeps returning values != -1 from peek();
                    // this causes the loop to never finish
                    // Propagating the exception (either EOF or socket closed) causes the loop to be aborted
                    // in this case
                    if (edde.getCause() instanceof IOException) {
                        throw edde;
                    }

                    EventHeaderV4 header = new EventHeaderV4();
                    header.setEventType(EventType.INCIDENT);
                    header.setTimestamp(edde.getEventHeader().getTimestamp());
                    header.setServerId(edde.getEventHeader().getServerId());

                    if (edde.getEventHeader() instanceof EventHeaderV4) {
                        header.setEventLength(((EventHeaderV4) edde.getEventHeader()).getEventLength());
                        header.setNextPosition(((EventHeaderV4) edde.getEventHeader()).getNextPosition());
                        header.setFlags(((EventHeaderV4) edde.getEventHeader()).getFlags());
                    }

                    EventData data = new EventDataDeserializationExceptionData(edde);
                    return new Event(header, data);
                }
            }
        };

        // Add our custom deserializers ...
        eventDeserializer.setEventDataDeserializer(EventType.STOP, new StopEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.GTID, new GtidEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(EventType.UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(EventType.DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(tableMapEventByTableId));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(
                        tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(
                        tableMapEventByTableId).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(
                        tableMapEventByTableId).setMayContainExtraInformation(true));
        client.setEventDeserializer(eventDeserializer);

        // Set up for JMX ...
        metrics = new BinlogReaderMetrics(client, context, name, changeEventQueueMetrics);
        heartbeat = Heartbeat.create(context.getConnectorConfig().getHeartbeatInterval(),
                context.topicSelector().getHeartbeatTopic(), context.getConnectorConfig().getLogicalName());
    }

    @Override
    protected void doInitialize() {
        metrics.register(logger);
    }

    @Override
    public void doDestroy() {
        metrics.unregister(logger);
    }

    @Override
    protected void doStart() {
        if (context.snapshotMode() != MySqlConnectorConfig.SnapshotMode.NEVER) {
            context.dbSchema().assureNonEmptySchema();
        }
        Set<Operation> skippedOperations = context.getConnectorConfig().getSkippedOperations();

        // Register our event handlers ...
        eventHandlers.put(EventType.STOP, this::handleServerStop);
        eventHandlers.put(EventType.HEARTBEAT, this::handleServerHeartbeat);
        eventHandlers.put(EventType.INCIDENT, this::handleServerIncident);
        eventHandlers.put(EventType.ROTATE, this::handleRotateLogsEvent);
        eventHandlers.put(EventType.TABLE_MAP, this::handleUpdateTableMetadata);
        eventHandlers.put(EventType.QUERY, this::handleQueryEvent);

        if (!skippedOperations.contains(Operation.CREATE)) {
            eventHandlers.put(EventType.WRITE_ROWS, this::handleInsert);
            eventHandlers.put(EventType.EXT_WRITE_ROWS, this::handleInsert);
        }

        if (!skippedOperations.contains(Operation.UPDATE)) {
            eventHandlers.put(EventType.UPDATE_ROWS, this::handleUpdate);
            eventHandlers.put(EventType.EXT_UPDATE_ROWS, this::handleUpdate);
        }

        if (!skippedOperations.contains(Operation.DELETE)) {
            eventHandlers.put(EventType.DELETE_ROWS, this::handleDelete);
            eventHandlers.put(EventType.EXT_DELETE_ROWS, this::handleDelete);
        }

        eventHandlers.put(EventType.VIEW_CHANGE, this::viewChange);
        eventHandlers.put(EventType.XA_PREPARE, this::prepareTransaction);
        eventHandlers.put(EventType.XID, this::handleTransactionCompletion);

        // Conditionally register ROWS_QUERY handler to parse SQL statements.
        if (context.includeSqlQuery()) {
            eventHandlers.put(EventType.ROWS_QUERY, this::handleRowsQuery);
        }

        final boolean isGtidModeEnabled = connectionContext.isGtidModeEnabled();
        metrics.setIsGtidModeEnabled(isGtidModeEnabled);

        // Get the current GtidSet from MySQL so we can get a filtered/merged GtidSet based off of the last Debezium checkpoint.
        String availableServerGtidStr = connectionContext.knownGtidSet();
        if (isGtidModeEnabled) {
            // The server is using GTIDs, so enable the handler ...
            eventHandlers.put(EventType.GTID, this::handleGtidEvent);

            // Now look at the GTID set from the server and what we've previously seen ...
            GtidSet availableServerGtidSet = new GtidSet(availableServerGtidStr);

            // also take into account purged GTID logs
            GtidSet purgedServerGtidSet = connectionContext.purgedGtidSet();
            logger.info("GTID set purged on server: {}", purgedServerGtidSet);

            GtidSet filteredGtidSet = context.filterGtidSet(availableServerGtidSet, purgedServerGtidSet);
            if (filteredGtidSet != null) {
                // We've seen at least some GTIDs, so start reading from the filtered GTID set ...
                logger.info("Registering binlog reader with GTID set: {}", filteredGtidSet);
                String filteredGtidSetStr = filteredGtidSet.toString();
                client.setGtidSet(filteredGtidSetStr);
                source.setCompletedGtidSet(filteredGtidSetStr);
                gtidSet = new com.github.shyiko.mysql.binlog.GtidSet(filteredGtidSetStr);
            }
            else {
                // We've not yet seen any GTIDs, so that means we have to start reading the binlog from the beginning ...
                client.setBinlogFilename(source.binlogFilename());
                client.setBinlogPosition(source.binlogPosition());
                gtidSet = new com.github.shyiko.mysql.binlog.GtidSet("");
            }
        }
        else {
            // The server is not using GTIDs, so start reading the binlog based upon where we last left off ...
            client.setBinlogFilename(source.binlogFilename());
            client.setBinlogPosition(source.binlogPosition());
        }

        // We may be restarting in the middle of a transaction, so see how far into the transaction we have already processed...
        initialEventsToSkip = source.eventsToSkipUponRestart();

        // Set the starting row number, which is the next row number to be read ...
        startingRowNumber = source.rowsToSkipUponRestart();

        // Only when we reach the first BEGIN event will we start to skip events ...
        skipEvent = false;

        // Initial our poll output delay logic ...
        pollOutputDelay.hasElapsed();
        previousOutputMillis = clock.currentTimeInMillis();

        // Start the log reader, which starts background threads ...
        if (isRunning()) {
            long timeout = context.getConnectorConfig().getConnectionTimeout().toMillis();
            long started = context.getClock().currentTimeInMillis();
            try {
                logger.debug("Attempting to establish binlog reader connection with timeout of {} ms", timeout);
                client.connect(timeout);
                // Need to wait for keepalive thread to be running, otherwise it can be left orphaned
                // The problem is with timing. When the close is called too early after connect then
                // the keepalive thread is not terminated
                if (client.isKeepAlive()) {
                    logger.info("Waiting for keepalive thread to start");
                    final Metronome metronome = Metronome.parker(Duration.ofMillis(100), clock);
                    int waitAttempts = 50;
                    boolean keepAliveThreadRunning = false;
                    while (!keepAliveThreadRunning && waitAttempts-- > 0) {
                        for (Thread t : binaryLogClientThreads.values()) {
                            if (t.getName().startsWith(KEEPALIVE_THREAD_NAME) && t.isAlive()) {
                                logger.info("Keepalive thread is running");
                                keepAliveThreadRunning = true;
                            }
                        }
                        metronome.pause();
                    }
                }
            }
            catch (TimeoutException e) {
                // If the client thread is interrupted *before* the client could connect, the client throws a timeout exception
                // The only way we can distinguish this is if we get the timeout exception before the specified timeout has
                // elapsed, so we simply check this (within 10%) ...
                long duration = context.getClock().currentTimeInMillis() - started;
                if (duration > (0.9 * timeout)) {
                    double actualSeconds = TimeUnit.MILLISECONDS.toSeconds(duration);
                    throw new ConnectException("Timed out after " + actualSeconds + " seconds while waiting to connect to MySQL at " +
                            connectionContext.hostname() + ":" + connectionContext.port() + " with user '" + connectionContext.username() + "'", e);
                }
                // Otherwise, we were told to shutdown, so we don't care about the timeout exception
            }
            catch (AuthenticationException e) {
                throw new ConnectException("Failed to authenticate to the MySQL database at " +
                        connectionContext.hostname() + ":" + connectionContext.port() + " with user '" + connectionContext.username() + "'", e);
            }
            catch (Throwable e) {
                throw new ConnectException("Unable to connect to the MySQL database at " +
                        connectionContext.hostname() + ":" + connectionContext.port() + " with user '" + connectionContext.username() + "': " + e.getMessage(), e);
            }
        }
    }

    protected void rewindBinaryLogClient(BinlogPosition position) {
        try {
            if (isRunning()) {
                logger.debug("Rewinding binlog to position {}", position);
                client.disconnect();
                client.setBinlogFilename(position.getFilename());
                client.setBinlogPosition(position.getPosition());
                client.connect();
            }
        }
        catch (IOException e) {
            logger.error("Unexpected error when re-connecting to the MySQL binary log reader", e);
        }
    }

    /**
     * @return a copy of the last offset of this reader, or null if this reader has not completed a poll.
     */
    public Map<String, ?> getLastOffset() {
        return lastOffset == null ? null : new HashMap<>(lastOffset);
    }

    @Override
    protected void doStop() {
        try {
            if (client.isConnected()) {
                logger.debug("Stopping binlog reader '{}', last recorded offset: {}", this.name(), lastOffset);
                client.disconnect();
            }
            cleanupResources();
        }
        catch (IOException e) {
            logger.error("Unexpected error when disconnecting from the MySQL binary log reader '{}'", this.name(), e);
        }
    }

    @Override
    protected void doCleanup() {
        logger.debug("Completed writing all records that were read from the binlog before being stopped");
    }

    @Override
    protected void pollComplete(List<SourceRecord> batch) {
        // Record a bit about this batch ...
        int batchSize = batch.size();
        recordCounter += batchSize;
        totalRecordCounter.addAndGet(batchSize);
        if (batchSize > 0) {
            SourceRecord lastRecord = batch.get(batchSize - 1);
            lastOffset = lastRecord.sourceOffset();
            if (pollOutputDelay.hasElapsed()) {
                // We want to record the status ...
                long millisSinceLastOutput = clock.currentTimeInMillis() - previousOutputMillis;
                try {
                    if (logger.isInfoEnabled()) {
                        context.temporaryLoggingContext("binlog", () -> {
                            logger.info("{} records sent during previous {}, last recorded offset: {}",
                                    recordCounter, Strings.duration(millisSinceLastOutput), lastOffset);
                        });
                    }
                }
                finally {
                    recordCounter = 0;
                    previousOutputMillis += millisSinceLastOutput;
                }
            }
        }
    }

    protected void logEvent(Event event) {
        logger.trace("Received event: {}", event);
    }

    protected void onEvent(Event event) {
        long ts = 0;

        if (event.getHeader().getEventType() == EventType.HEARTBEAT) {
            // HEARTBEAT events have no timestamp but are fired only when
            // there is no traffic on the connection which means we are caught-up
            // https://dev.mysql.com/doc/internals/en/heartbeat-event.html
            metrics.setMilliSecondsBehindSource(ts);
            return;
        }

        // MySQL has seconds resolution but mysql-binlog-connector-java returns
        // a value in milliseconds
        long eventTs = event.getHeader().getTimestamp();

        if (eventTs == 0) {
            logger.trace("Received unexpected event with 0 timestamp: {}", event);
            return;
        }

        ts = clock.currentTimeInMillis() - eventTs;
        logger.trace("Current milliseconds behind source: {} ms", ts);
        metrics.setMilliSecondsBehindSource(ts);
    }

    protected void ignoreEvent(Event event) {
        logger.trace("Ignoring event due to missing handler: {}", event);
    }

    protected void handleEvent(Event event) {
        if (event == null) {
            return;
        }

        // Update the source offset info. Note that the client returns the value in *milliseconds*, even though the binlog
        // contains only *seconds* precision ...
        EventHeader eventHeader = event.getHeader();
        if (!eventHeader.getEventType().equals(EventType.HEARTBEAT)) {
            // HEARTBEAT events have no timestamp; only set the timestamp if the event is not a HEARTBEAT
            source.setBinlogTimestampSeconds(eventHeader.getTimestamp() / 1000L); // client returns milliseconds,
                                                                                  // but only second precision
        }

        source.setBinlogServerId(eventHeader.getServerId());
        EventType eventType = eventHeader.getEventType();
        if (eventType == EventType.ROTATE) {
            EventData eventData = event.getData();
            RotateEventData rotateEventData;
            if (eventData instanceof EventDeserializer.EventDataWrapper) {
                rotateEventData = (RotateEventData) ((EventDeserializer.EventDataWrapper) eventData).getInternal();
            }
            else {
                rotateEventData = (RotateEventData) eventData;
            }
            source.setBinlogStartPoint(rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
        }
        else if (eventHeader instanceof EventHeaderV4) {
            EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
            source.setEventPosition(trackableEventHeader.getPosition(), trackableEventHeader.getEventLength());
        }

        // If there is a handler for this event, forward the event to it ...
        try {
            // Forward the event to the handler ...
            eventHandlers.getOrDefault(eventType, this::ignoreEvent).accept(event);

            // Generate heartbeat message if the time is right
            heartbeat.heartbeat(source.partition(), source.offset(), (BlockingConsumer<SourceRecord>) this::enqueueRecord);

            // Capture that we've completed another event ...
            source.completeEvent();

            if (skipEvent) {
                // We're in the mode of skipping events and we just skipped this one, so decrement our skip count ...
                --initialEventsToSkip;
                skipEvent = initialEventsToSkip > 0;
            }
        }
        catch (RuntimeException e) {
            // There was an error in the event handler, so propagate the failure to Kafka Connect ...
            logReaderState();
            failed(e, "Error processing binlog event");
            // Do not stop the client, since Kafka Connect should stop the connector on it's own
            // (and doing it here may cause problems the second time it is stopped).
            // We can clear the listeners though so that we ignore all future events ...
            eventHandlers.clear();
            logger.info(
                    "Error processing binlog event, and propagating to Kafka Connect so it stops this connector. Future binlog events read before connector is shutdown will be ignored.");
        }
        catch (InterruptedException e) {
            // Most likely because this reader was stopped and our thread was interrupted ...
            Thread.currentThread().interrupt();
            eventHandlers.clear();
            logger.info("Stopped processing binlog events due to thread interruption");
        }
    }

    @SuppressWarnings("unchecked")
    protected <T extends EventData> T unwrapData(Event event) {
        EventData eventData = event.getData();
        if (eventData instanceof EventDeserializer.EventDataWrapper) {
            eventData = ((EventDeserializer.EventDataWrapper) eventData).getInternal();
        }
        return (T) eventData;
    }

    /**
     * Handle the supplied event that signals that mysqld has stopped.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerStop(Event event) {
        logger.debug("Server stopped: {}", event);
    }

    /**
     * Handle the supplied event that is sent by a primary to a replica to let the replica know that the primary is still alive. Not
     * written to a binary log.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerHeartbeat(Event event) {
        logger.trace("Server heartbeat: {}", event);
    }

    /**
     * Handle the supplied event that signals that an out of the ordinary event that occurred on the master. It notifies the replica
     * that something happened on the primary that might cause data to be in an inconsistent state.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerIncident(Event event) {
        if (event.getData() instanceof EventDataDeserializationExceptionData) {
            metrics.onErroneousEvent("source = " + event.toString());
            EventDataDeserializationExceptionData data = event.getData();

            EventHeaderV4 eventHeader = (EventHeaderV4) data.getCause().getEventHeader(); // safe cast, instantiated that ourselves

            // logging some additional context but not the exception itself, this will happen in handleEvent()
            if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                logger.error(
                        "Error while deserializing binlog event at offset {}.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        source.offset(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        source.binlogFilename());

                throw new RuntimeException(data.getCause());
            }
            else if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                logger.warn(
                        "Error while deserializing binlog event at offset {}.{}" +
                                "This exception will be ignored and the event be skipped.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        source.offset(),
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        source.binlogFilename(),
                        data.getCause());
            }
        }
        else {
            logger.error("Server incident: {}", event);
        }
    }

    /**
     * Handle the supplied event with a {@link RotateEventData} that signals the logs are being rotated. This means that either
     * the server was restarted, or the binlog has transitioned to a new file. In either case, subsequent table numbers will be
     * different than those seen to this point, so we need to {@link RecordMakers#clear() discard the cache of record makers}.
     *
     * @param event the database change data event to be processed; may not be null
     */
    protected void handleRotateLogsEvent(Event event) {
        logger.debug("Rotating logs: {}", event);
        RotateEventData command = unwrapData(event);
        assert command != null;
        recordMakers.clear();
    }

    /**
     * Handle the supplied event with a {@link GtidEventData} that signals the beginning of a GTID transaction.
     * We don't yet know whether this transaction contains any events we're interested in, but we have to record
     * it so that we know the position of this event and know we've processed the binlog to this point.
     * <p>
     * Note that this captures the current GTID and complete GTID set, regardless of whether the connector is
     * {@link MySqlTaskContext#gtidSourceFilter() filtering} the GTID set upon connection. We do this because
     * we actually want to capture all GTID set values found in the binlog, whether or not we process them.
     * However, only when we connect do we actually want to pass to MySQL only those GTID ranges that are applicable
     * per the configuration.
     *
     * @param event the GTID event to be processed; may not be null
     */
    protected void handleGtidEvent(Event event) {
        logger.debug("GTID transaction: {}", event);
        GtidEventData gtidEvent = unwrapData(event);
        String gtid = gtidEvent.getGtid();
        gtidSet.add(gtid);
        source.startGtid(gtid, gtidSet.toString()); // rather than use the client's GTID set
        ignoreDmlEventByGtidSource = false;
        if (gtidDmlSourceFilter != null && gtid != null) {
            String uuid = gtid.trim().substring(0, gtid.indexOf(":"));
            if (!gtidDmlSourceFilter.test(uuid)) {
                ignoreDmlEventByGtidSource = true;
            }
        }
        metrics.onGtidChange(gtid);
    }

    /**
     * Handle the supplied event with an {@link RowsQueryEventData} by recording the original SQL query
     * that generated the event.
     *
     * @param event the database change data event to be processed; may not be null
     */
    protected void handleRowsQuery(Event event) {
        // Unwrap the RowsQueryEvent
        final RowsQueryEventData lastRowsQueryEventData = unwrapData(event);

        // Set the query on the source
        source.setQuery(lastRowsQueryEventData.getQuery());
    }

    /**
     * Handle the supplied event with an {@link QueryEventData} by possibly recording the DDL statements as changes in the
     * MySQL schemas.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while recording the DDL statements
     */
    protected void handleQueryEvent(Event event) throws InterruptedException {
        QueryEventData command = unwrapData(event);
        logger.debug("Received query command: {}", event);
        String sql = command.getSql().trim();
        if (sql.equalsIgnoreCase("BEGIN")) {
            // We are starting a new transaction ...
            source.startNextTransaction();
            source.setBinlogThread(command.getThreadId());
            if (initialEventsToSkip != 0) {
                logger.debug("Restarting partially-processed transaction; change events will not be created for the first {} events plus {} more rows in the next event",
                        initialEventsToSkip, startingRowNumber);
                // We are restarting, so we need to skip the events in this transaction that we processed previously...
                skipEvent = true;
            }
            return;
        }
        if (sql.equalsIgnoreCase("COMMIT")) {
            handleTransactionCompletion(event);
            return;
        }

        String upperCasedStatementBegin = Strings.getBegin(sql, 7).toUpperCase();

        if (upperCasedStatementBegin.startsWith("XA ")) {
            // This is an XA transaction, and we currently ignore these and do nothing ...
            return;
        }
        if (context.ddlFilter().test(sql)) {
            logger.debug("DDL '{}' was filtered out of processing", sql);
            return;
        }
        if (upperCasedStatementBegin.equals("INSERT ") || upperCasedStatementBegin.equals("UPDATE ") || upperCasedStatementBegin.equals("DELETE ")) {
            if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                throw new ConnectException(
                        "Received DML '" + sql + "' for processing, binlog probably contains events generated with statement or mixed based replication format");
            }
            else if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                logger.warn("Warning only: Received DML '" + sql
                        + "' for processing, binlog probably contains events generated with statement or mixed based replication format");
                return;
            }
            else {
                return;
            }
        }
        if (sql.equalsIgnoreCase("ROLLBACK")) {
            // We have hit a ROLLBACK which is not supported
            logger.warn("Rollback statements cannot be handled without binlog buffering, the connector will fail. Please check '{}' to see how to enable buffering",
                    MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER.name());
        }
        context.dbSchema().applyDdl(context.source(), command.getDatabase(), command.getSql(), (dbName, tables, statements) -> {
            if (recordSchemaChangesInSourceRecords && recordMakers.schemaChanges(dbName, tables, statements, super::enqueueRecord) > 0) {
                logger.debug("Recorded DDL statements for database '{}': {}", dbName, statements);
            }
        });
    }

    private void handleTransactionCompletion(Event event) {
        // We are completing the transaction ...
        source.commitTransaction();
        source.setBinlogThread(-1L);
        skipEvent = false;
        ignoreDmlEventByGtidSource = false;
    }

    /**
     * Handle a change in the table metadata.
     * <p>
     * This method should be called whenever we consume a TABLE_MAP event, and every transaction in the log should include one
     * of these for each table affected by the transaction. Each table map event includes a monotonically-increasing numeric
     * identifier, and this identifier is used within subsequent events within the same transaction. This table identifier can
     * change when:
     * <ol>
     * <li>the table structure is modified (e.g., via an {@code ALTER TABLE ...} command); or</li>
     * <li>MySQL rotates to a new binary log file, even if the table structure does not change.</li>
     * </ol>
     *
     * @param event the update event; never null
     */
    protected void handleUpdateTableMetadata(Event event) {
        TableMapEventData metadata = unwrapData(event);
        long tableNumber = metadata.getTableId();
        String databaseName = metadata.getDatabase();
        String tableName = metadata.getTable();
        TableId tableId = new TableId(databaseName, null, tableName);
        if (recordMakers.assign(tableNumber, tableId)) {
            logger.debug("Received update table metadata event: {}", event);
        }
        else {
            informAboutUnknownTableIfRequired(event, tableId, "update table metadata");
        }
    }

    /**
     * If we receive an event for a table that is monitored but whose metadata we
     * don't know, either ignore that event or raise a warning or error as per the
     * {@link MySqlConnectorConfig#INCONSISTENT_SCHEMA_HANDLING_MODE} configuration.
     */
    private void informAboutUnknownTableIfRequired(Event event, TableId tableId, String typeToLog) {
        if (tableId != null && context.dbSchema().isTableCaptured(tableId)) {
            metrics.onErroneousEvent("source = " + tableId + ", event " + event);
            EventHeaderV4 eventHeader = event.getHeader();

            if (inconsistentSchemaHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                logger.error(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}"
                                +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        source.offset(),
                        tableId,
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        source.binlogFilename());
                throw new ConnectException("Encountered change event for table " + tableId + " whose schema isn't known to this connector");
            }
            else if (inconsistentSchemaHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                logger.warn(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}"
                                +
                                "The event will be ignored.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        source.offset(),
                        tableId,
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        source.binlogFilename());
            }
            else {
                logger.debug(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database history topic. Take a new snapshot in this case.{}"
                                +
                                "The event will be ignored.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event,
                        source.offset(),
                        tableId,
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        source.binlogFilename());
            }
        }
        else {
            logger.debug("Filtering {} event: {} for non-monitored table {}", typeToLog, event, tableId);
            metrics.onFilteredEvent("source = " + tableId);
        }
    }

    /**
     * Generate source records for the supplied event with an {@link WriteRowsEventData}.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleInsert(Event event) throws InterruptedException {
        if (skipEvent) {
            // We can skip this because we should already be at least this far ...
            logger.debug("Skipping previously processed row event: {}", event);
            return;
        }
        if (ignoreDmlEventByGtidSource) {
            logger.debug("Skipping DML event because this GTID source is filtered: {}", event);
            return;
        }
        WriteRowsEventData write = unwrapData(event);
        long tableNumber = write.getTableId();
        BitSet includedColumns = write.getIncludedColumns();
        RecordsForTable recordMaker = recordMakers.forTable(tableNumber, includedColumns, super::enqueueRecord);
        if (recordMaker != null) {
            List<Serializable[]> rows = write.getRows();
            final Instant ts = context.getClock().currentTimeAsInstant();
            int count = 0;
            int numRows = rows.size();
            if (startingRowNumber < numRows) {
                for (int row = startingRowNumber; row != numRows; ++row) {
                    count += recordMaker.create(rows.get(row), ts, row, numRows);
                }
                if (logger.isDebugEnabled()) {
                    if (startingRowNumber != 0) {
                        logger.debug("Recorded {} insert record(s) for last {} row(s) in event: {}",
                                count, numRows - startingRowNumber, event);
                    }
                    else {
                        logger.debug("Recorded {} insert record(s) for event: {}", count, event);
                    }
                }
            }
            else {
                // All rows were previously processed ...
                logger.debug("Skipping previously processed insert event: {}", event);
            }
        }
        else {
            informAboutUnknownTableIfRequired(event, recordMakers.getTableIdFromTableNumber(tableNumber), "insert row");
        }
        startingRowNumber = 0;
    }

    /**
     * Generate source records for the supplied event with an {@link UpdateRowsEventData}.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleUpdate(Event event) throws InterruptedException {
        if (skipEvent) {
            // We can skip this because we should already be at least this far ...
            logger.debug("Skipping previously processed row event: {}", event);
            return;
        }
        if (ignoreDmlEventByGtidSource) {
            logger.debug("Skipping DML event because this GTID source is filtered: {}", event);
            return;
        }
        UpdateRowsEventData update = unwrapData(event);
        long tableNumber = update.getTableId();
        BitSet includedColumns = update.getIncludedColumns();
        // BitSet includedColumnsBefore = update.getIncludedColumnsBeforeUpdate();
        RecordsForTable recordMaker = recordMakers.forTable(tableNumber, includedColumns, super::enqueueRecord);
        if (recordMaker != null) {
            List<Entry<Serializable[], Serializable[]>> rows = update.getRows();
            final Instant ts = context.getClock().currentTimeAsInstant();
            int count = 0;
            int numRows = rows.size();
            if (startingRowNumber < numRows) {
                for (int row = startingRowNumber; row != numRows; ++row) {
                    Map.Entry<Serializable[], Serializable[]> changes = rows.get(row);
                    Serializable[] before = changes.getKey();
                    Serializable[] after = changes.getValue();
                    count += recordMaker.update(before, after, ts, row, numRows);
                }
                if (logger.isDebugEnabled()) {
                    if (startingRowNumber != 0) {
                        logger.debug("Recorded {} update record(s) for last {} row(s) in event: {}",
                                count, numRows - startingRowNumber, event);
                    }
                    else {
                        logger.debug("Recorded {} update record(s) for event: {}", count, event);
                    }
                }
            }
            else {
                // All rows were previously processed ...
                logger.debug("Skipping previously processed update event: {}", event);
            }
        }
        else {
            informAboutUnknownTableIfRequired(event, recordMakers.getTableIdFromTableNumber(tableNumber), "update row");
        }
        startingRowNumber = 0;
    }

    /**
     * Generate source records for the supplied event with an {@link DeleteRowsEventData}.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleDelete(Event event) throws InterruptedException {
        if (skipEvent) {
            // We can skip this because we should already be at least this far ...
            logger.debug("Skipping previously processed row event: {}", event);
            return;
        }
        if (ignoreDmlEventByGtidSource) {
            logger.debug("Skipping DML event because this GTID source is filtered: {}", event);
            return;
        }
        DeleteRowsEventData deleted = unwrapData(event);
        long tableNumber = deleted.getTableId();
        BitSet includedColumns = deleted.getIncludedColumns();
        RecordsForTable recordMaker = recordMakers.forTable(tableNumber, includedColumns, super::enqueueRecord);
        if (recordMaker != null) {
            List<Serializable[]> rows = deleted.getRows();
            final Instant ts = context.getClock().currentTimeAsInstant();
            int count = 0;
            int numRows = rows.size();
            if (startingRowNumber < numRows) {
                for (int row = startingRowNumber; row != numRows; ++row) {
                    count += recordMaker.delete(rows.get(row), ts, row, numRows);
                }
                if (logger.isDebugEnabled()) {
                    if (startingRowNumber != 0) {
                        logger.debug("Recorded {} delete record(s) for last {} row(s) in event: {}",
                                count, numRows - startingRowNumber, event);
                    }
                    else {
                        logger.debug("Recorded {} delete record(s) for event: {}", count, event);
                    }
                }
            }
            else {
                // All rows were previously processed ...
                logger.debug("Skipping previously processed delete event: {}", event);
            }
        }
        else {
            informAboutUnknownTableIfRequired(event, recordMakers.getTableIdFromTableNumber(tableNumber), "delete row");
        }
        startingRowNumber = 0;
    }

    /**
     * Handle a {@link EventType#VIEW_CHANGE} event.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void viewChange(Event event) throws InterruptedException {
        logger.debug("View Change event: {}", event);
        // do nothing
    }

    /**
     * Handle a {@link EventType#XA_PREPARE} event.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void prepareTransaction(Event event) throws InterruptedException {
        logger.debug("XA Prepare event: {}", event);
        // do nothing
    }

    protected static SSLMode sslModeFor(SecureConnectionMode mode) {
        switch (mode) {
            case DISABLED:
                return SSLMode.DISABLED;
            case PREFERRED:
                return SSLMode.PREFERRED;
            case REQUIRED:
                return SSLMode.REQUIRED;
            case VERIFY_CA:
                return SSLMode.VERIFY_CA;
            case VERIFY_IDENTITY:
                return SSLMode.VERIFY_IDENTITY;
        }
        return null;
    }

    protected final class ReaderThreadLifecycleListener implements LifecycleListener {
        @Override
        public void onDisconnect(BinaryLogClient client) {
            if (logger.isInfoEnabled()) {
                context.temporaryLoggingContext("binlog", () -> {
                    Map<String, ?> offset = lastOffset;
                    if (offset != null) {
                        logger.info("Stopped reading binlog after {} events, last recorded offset: {}", totalRecordCounter, offset);
                    }
                    else {
                        logger.info("Stopped reading binlog after {} events, no new offset was recorded", totalRecordCounter);
                    }
                });
            }
        }

        @Override
        public void onConnect(BinaryLogClient client) {
            // Set up the MDC logging context for this thread ...
            context.configureLoggingContext("binlog");

            // The event row number will be used when processing the first event ...
            logger.info("Connected to MySQL binlog at {}:{}, starting at {}", connectionContext.hostname(), connectionContext.port(), source);
        }

        @Override
        public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
            logger.debug("A communication failure event arrived", ex);
            logReaderState();
            try {
                // Stop BinaryLogClient background threads
                client.disconnect();
            }
            catch (final Exception e) {
                logger.debug("Exception while closing client", e);
            }
            BinlogReader.this.failed(ex);
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
            if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                logger.debug("A deserialization failure event arrived", ex);
                logReaderState();
                BinlogReader.this.failed(ex);
            }
            else if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                logger.warn("A deserialization failure event arrived", ex);
                logReaderState(Level.WARN);
            }
            else {
                logger.debug("A deserialization failure event arrived", ex);
                logReaderState(Level.DEBUG);
            }
        }
    }

    private void logReaderState() {
        logReaderState(Level.ERROR);
    }

    private void logReaderState(Level severity) {
        final Object position = client == null ? "N/A" : client.getBinlogFilename() + "/" + client.getBinlogPosition();
        final String message = "Error during binlog processing. Last offset stored = {}, binlog reader near position = {}";
        switch (severity) {
            case WARN:
                logger.warn(message, lastOffset, position);
                break;
            case DEBUG:
                logger.debug(message, lastOffset, position);
                break;
            default:
                logger.error(message, lastOffset, position);
        }
    }

    protected BinlogReaderMetrics getMetrics() {
        return metrics;
    }

    protected BinaryLogClient getBinlogClient() {
        return client;
    }

    public BinlogPosition getCurrentBinlogPosition() {
        return new BinlogPosition(client.getBinlogFilename(), client.getBinlogPosition());
    }

    private SSLSocketFactory getBinlogSslSocketFactory(MySqlJdbcContext connectionContext) {
        String acceptedTlsVersion = connectionContext.getSessionVariableForSslVersion();
        if (!isNullOrEmpty(acceptedTlsVersion)) {
            SSLMode sslMode = sslModeFor(connectionContext.sslMode());

            // Keystore settings can be passed via system properties too so we need to read them
            final String password = System.getProperty("javax.net.ssl.keyStorePassword");
            final String keyFilename = System.getProperty("javax.net.ssl.keyStore");
            KeyManager[] keyManagers = null;
            if (keyFilename != null) {
                final char[] passwordArray = (password == null) ? null : password.toCharArray();
                try {
                    KeyStore ks = KeyStore.getInstance("JKS");
                    ks.load(new FileInputStream(keyFilename), passwordArray);

                    KeyManagerFactory kmf = KeyManagerFactory.getInstance("NewSunX509");
                    kmf.init(ks, passwordArray);

                    keyManagers = kmf.getKeyManagers();
                }
                catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
                    throw new ConnectException("Could not load keystore", e);
                }
            }

            // DBZ-1208 Resembles the logic from the upstream BinaryLogClient, only that
            // the accepted TLS version is passed to the constructed factory
            if (sslMode == SSLMode.PREFERRED || sslMode == SSLMode.REQUIRED) {
                final KeyManager[] finalKMS = keyManagers;
                return new DefaultSSLSocketFactory(acceptedTlsVersion) {

                    @Override
                    protected void initSSLContext(SSLContext sc)
                            throws GeneralSecurityException {
                        sc.init(finalKMS, new TrustManager[]{
                                new X509TrustManager() {

                                    @Override
                                    public void checkClientTrusted(
                                                                   X509Certificate[] x509Certificates,
                                                                   String s)
                                            throws CertificateException {
                                    }

                                    @Override
                                    public void checkServerTrusted(
                                                                   X509Certificate[] x509Certificates,
                                                                   String s)
                                            throws CertificateException {
                                    }

                                    @Override
                                    public X509Certificate[] getAcceptedIssuers() {
                                        return new X509Certificate[0];
                                    }
                                }
                        }, null);
                    }
                };
            }
            else {
                return new DefaultSSLSocketFactory(acceptedTlsVersion);
            }
        }

        return null;
    }
}

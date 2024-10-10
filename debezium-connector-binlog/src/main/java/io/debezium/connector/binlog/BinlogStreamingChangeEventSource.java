/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.util.Strings.isNullOrEmpty;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TransactionPayloadEventData;
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
import com.github.shyiko.mysql.binlog.network.ServerException;

import io.debezium.DebeziumException;
import io.debezium.annotation.SingleThreadAccess;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SecureConnectionMode;
import io.debezium.connector.binlog.event.EventDataDeserializationExceptionData;
import io.debezium.connector.binlog.event.RowDeserializers;
import io.debezium.connector.binlog.event.StopEventDataDeserializer;
import io.debezium.connector.binlog.event.TransactionPayloadDeserializer;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.metrics.BinlogStreamingChangeEventSourceMetrics;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.mode.NeverSnapshotter;
import io.debezium.time.Conversions;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * An abstract common implementation of {@link StreamingChangeEventSource} for binlog-based connectors.
 *
 * @author Jiri Pechanec
 * @author Chris Cranford
 */
public abstract class BinlogStreamingChangeEventSource<P extends BinlogPartition, O extends BinlogOffsetContext>
        implements StreamingChangeEventSource<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogStreamingChangeEventSource.class);

    private static final String KEEPALIVE_THREAD_NAME = "blc-keepalive";
    private static final String SET_STATEMENT_REGEX = "SET STATEMENT .* FOR";

    private final BinaryLogClient client;
    private final BinlogStreamingChangeEventSourceMetrics<?, P> metrics;
    // todo: can we go back to accessing schema via task context? issue is all the generics :/
    private final BinlogTaskContext<?> taskContext;
    private final BinlogDatabaseSchema schema;
    private final BinlogConnectorConfig connectorConfig;
    private final BinlogConnectorConnection connection;
    private final EventDispatcher<P, TableId> eventDispatcher;
    private final Clock clock;
    private final ErrorHandler errorHandler;
    private final EventProcessingFailureHandlingMode eventDeserializationFailureHandlingMode;
    private final EventProcessingFailureHandlingMode inconsistentSchemaHandlingMode;
    private final SnapshotterService snapshotterService;
    private final Predicate<String> gtidDmlSourceFilter;
    private final boolean isGtidModeEnabled;
    private final AtomicLong totalRecordCounter = new AtomicLong();
    private final Map<String, Thread> binaryLogClientThreads = new ConcurrentHashMap<>(4);
    private final EnumMap<EventType, BlockingConsumer<Event>> eventHandlers = new EnumMap<>(EventType.class);
    private final float heartbeatIntervalFactor = 0.8f;

    private int startingRowNumber = 0;
    private long initialEventsToSkip = 0L;
    private boolean skipEvent = false;
    private boolean ignoreDmlEventByGtidSource = false;
    private volatile Map<String, ?> lastOffset = null;
    private O effectiveOffsetContext;

    @SingleThreadAccess("binlog client thread")
    protected Instant eventTimestamp;

    public BinlogStreamingChangeEventSource(BinlogConnectorConfig connectorConfig,
                                            BinlogConnectorConnection connection,
                                            EventDispatcher<P, TableId> dispatcher,
                                            ErrorHandler errorHandler,
                                            Clock clock,
                                            BinlogTaskContext<?> taskContext,
                                            BinlogDatabaseSchema schema,
                                            BinlogStreamingChangeEventSourceMetrics<?, P> metrics,
                                            SnapshotterService snapshotterService) {
        this.taskContext = taskContext;
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.connection = connection;
        this.clock = clock;
        this.eventDispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.metrics = metrics;
        this.eventDeserializationFailureHandlingMode = connectorConfig.getEventProcessingFailureHandlingMode();
        this.inconsistentSchemaHandlingMode = connectorConfig.getInconsistentSchemaFailureHandlingMode();
        this.snapshotterService = snapshotterService;
        this.client = createBinaryLogClient(taskContext, connectorConfig, binaryLogClientThreads, connection);
        this.gtidDmlSourceFilter = getGtidDmlSourceFilter();
        this.isGtidModeEnabled = connection.isGtidModeEnabled();
    }

    @Override
    public void execute(ChangeEventSourceContext context, P partition, O offsetContext) throws InterruptedException {
        if (!(snapshotterService.getSnapshotter() instanceof NeverSnapshotter)) {
            taskContext.getSchema().assureNonEmptySchema();
        }
        final Set<Envelope.Operation> skippedOperations = connectorConfig.getSkippedOperations();

        // Register our event handlers ...
        eventHandlers.put(EventType.STOP, (event) -> handleServerStop(effectiveOffsetContext, event));
        eventHandlers.put(EventType.HEARTBEAT, (event) -> handleServerHeartbeat(partition, effectiveOffsetContext, event));
        eventHandlers.put(EventType.INCIDENT, (event) -> handleServerIncident(partition, effectiveOffsetContext, event));
        eventHandlers.put(EventType.ROTATE, (event) -> handleRotateLogsEvent(effectiveOffsetContext, event));
        eventHandlers.put(EventType.TABLE_MAP, (event) -> handleUpdateTableMetadata(partition, effectiveOffsetContext, event));
        eventHandlers.put(EventType.QUERY, (event) -> handleQueryEvent(partition, effectiveOffsetContext, event));
        eventHandlers.put(EventType.TRANSACTION_PAYLOAD, (event) -> handleTransactionPayload(partition, effectiveOffsetContext, event));

        if (!skippedOperations.contains(Envelope.Operation.CREATE)) {
            eventHandlers.put(EventType.WRITE_ROWS, (event) -> handleInsert(partition, effectiveOffsetContext, event));
            eventHandlers.put(EventType.EXT_WRITE_ROWS, (event) -> handleInsert(partition, effectiveOffsetContext, event));
        }

        if (!skippedOperations.contains(Envelope.Operation.UPDATE)) {
            eventHandlers.put(EventType.UPDATE_ROWS, (event) -> handleUpdate(partition, effectiveOffsetContext, event));
            eventHandlers.put(EventType.EXT_UPDATE_ROWS, (event) -> handleUpdate(partition, effectiveOffsetContext, event));
        }

        if (!skippedOperations.contains(Envelope.Operation.DELETE)) {
            eventHandlers.put(EventType.DELETE_ROWS, (event) -> handleDelete(partition, effectiveOffsetContext, event));
            eventHandlers.put(EventType.EXT_DELETE_ROWS, (event) -> handleDelete(partition, effectiveOffsetContext, event));
        }

        eventHandlers.put(EventType.VIEW_CHANGE, (event) -> viewChange(effectiveOffsetContext, event));
        eventHandlers.put(EventType.XA_PREPARE, (event) -> prepareTransaction(effectiveOffsetContext, event));
        eventHandlers.put(EventType.XID, (event) -> handleTransactionCompletion(partition, effectiveOffsetContext, event));

        // Conditionally register ROWS_QUERY handler to parse SQL statements.
        if (connectorConfig.isSqlQueryIncluded()) {
            final EventType eventType = getIncludeQueryEventType();
            eventHandlers.put(eventType, (event) -> handleRecordingQuery(effectiveOffsetContext, event));
        }

        BinaryLogClient.EventListener listener;
        if (connectorConfig.getBufferSizeForStreamingChangeEventSource() == 0) {
            listener = (event) -> handleEvent(partition, effectiveOffsetContext, context, event);
        }
        else {
            EventBuffer<?, P, O> buffer = new EventBuffer<>(connectorConfig.getBufferSizeForStreamingChangeEventSource(), this, context);
            listener = (event) -> buffer.add(partition, effectiveOffsetContext, event);
        }
        client.registerEventListener(listener);

        client.registerLifecycleListener(new ReaderThreadLifecycleListener(effectiveOffsetContext));
        client.registerEventListener((event) -> onEvent(effectiveOffsetContext, event));
        if (LOGGER.isDebugEnabled()) {
            client.registerEventListener((event) -> logEvent(effectiveOffsetContext, event));
        }

        metrics.setIsGtidModeEnabled(isGtidModeEnabled);

        // Get the current GtidSet from MySQL so we can get a filtered/merged GtidSet based off of the last Debezium checkpoint.
        if (isGtidModeEnabled) {
            // The server is using GTIDs, so enable the handler ...
            eventHandlers.put(getGtidEventType(),
                    (event) -> handleGtidEvent(partition, effectiveOffsetContext, event, gtidDmlSourceFilter));

            // Now look at the GTID set from the server and what we've previously seen ...
            GtidSet availableServerGtidSet = connection.knownGtidSet();

            // also take into account purged GTID logs
            GtidSet purgedServerGtidSet = connection.purgedGtidSet();
            LOGGER.info("GTID set purged on server: '{}'", purgedServerGtidSet);

            final GtidSet filteredGtidSet = connection.filterGtidSet(
                    connectorConfig.getGtidSourceFilter(),
                    effectiveOffsetContext.gtidSet(),
                    availableServerGtidSet,
                    purgedServerGtidSet);

            if (filteredGtidSet != null) {
                // We've seen at least some GTIDs, so start reading from the filtered GTID set ...
                LOGGER.info("Registering binlog reader with GTID set: '{}'", filteredGtidSet);
                String filteredGtidSetStr = filteredGtidSet.toString();
                client.setGtidSet(filteredGtidSetStr);
                effectiveOffsetContext.setCompletedGtidSet(filteredGtidSetStr);
                initializeGtidSet(filteredGtidSetStr);
            }
            else {
                // We've not yet seen any GTIDs, so that means we have to start reading the binlog from the beginning ...
                client.setBinlogFilename(effectiveOffsetContext.getSource().binlogFilename());
                client.setBinlogPosition(effectiveOffsetContext.getSource().binlogPosition());
                if (purgedServerGtidSet == null || purgedServerGtidSet.isEmpty()) {
                    LOGGER.info("No GTID stored in the offset, registering binlog reader with empty GTID set.");
                    client.setGtidSet("");
                    initializeGtidSet("");
                }
                else {
                    LOGGER.info("No GTID stored in the offset, but there is non-empty purged GTID set. Registering binlog reader with purged GTID set: '{}'",
                            purgedServerGtidSet.toString());
                    client.setGtidSet(purgedServerGtidSet.toString());
                    // We don't have stored any GTID in the offset, so start from empty GTID set.
                    initializeGtidSet("");
                }
            }
        }
        else {
            // The server is not using GTIDs, so start reading the binlog based upon where we last left off ...
            client.setBinlogFilename(effectiveOffsetContext.getSource().binlogFilename());
            client.setBinlogPosition(effectiveOffsetContext.getSource().binlogPosition());
        }

        // We may be restarting in the middle of a transaction, so see how far into the transaction we have already processed...
        initialEventsToSkip = effectiveOffsetContext.eventsToSkipUponRestart();
        LOGGER.info("Skip {} events on streaming start", initialEventsToSkip);

        // Set the starting row number, which is the next row number to be read ...
        startingRowNumber = effectiveOffsetContext.rowsToSkipUponRestart();
        LOGGER.info("Skip {} rows on streaming start", startingRowNumber);

        // Only when we reach the first BEGIN event will we start to skip events ...
        skipEvent = false;

        try {
            // Start the log reader, which starts background threads ...
            if (context.isRunning()) {
                long timeout = connectorConfig.getConnectionTimeout().toMillis();
                long started = clock.currentTimeInMillis();
                try {
                    LOGGER.debug("Attempting to establish binlog reader connection with timeout of {} ms", timeout);
                    client.connect(timeout);
                    // Need to wait for keepalive thread to be running, otherwise it can be left orphaned
                    // The problem is with timing. When the close is called too early after connect then
                    // the keepalive thread is not terminated
                    if (client.isKeepAlive()) {
                        LOGGER.info("Waiting for keepalive thread to start");
                        final Metronome metronome = Metronome.parker(Duration.ofMillis(100), clock);
                        int waitAttempts = 50;
                        boolean keepAliveThreadRunning = false;
                        while (!keepAliveThreadRunning && waitAttempts-- > 0) {
                            for (Thread t : binaryLogClientThreads.values()) {
                                if (t.getName().startsWith(KEEPALIVE_THREAD_NAME) && t.isAlive()) {
                                    LOGGER.info("Keepalive thread is running");
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
                    long duration = clock.currentTimeInMillis() - started;
                    if (duration > (0.9 * timeout)) {
                        double actualSeconds = TimeUnit.MILLISECONDS.toSeconds(duration);
                        throw new DebeziumException(String.format(
                                "Timed out after %f seconds while waiting to connect to database at %s:%d with user '%s'",
                                actualSeconds, connectorConfig.getHostName(), connectorConfig.getPort(), connectorConfig.getUserName()), e);
                    }
                    // Otherwise, we were told to shutdown, so we don't care about the timeout exception
                }
                catch (AuthenticationException e) {
                    throw new DebeziumException(String.format(
                            "Failed to authenticate to the database at %s:%d with user '%s'",
                            connectorConfig.getHostName(), connectorConfig.getPort(), connectorConfig.getUserName()), e);
                }
                catch (Throwable e) {
                    throw new DebeziumException(String.format(
                            "Unable to connect to the database at %s:%d with user '%s': %s",
                            connectorConfig.getHostName(), connectorConfig.getPort(), connectorConfig.getUserName(), e.getMessage()), e);
                }
            }
            while (context.isRunning()) {
                Thread.sleep(100);
                waitWhenStreamingPaused(context);
            }
        }
        finally {
            try {
                client.disconnect();
            }
            catch (Exception e) {
                LOGGER.info("Exception while stopping binary log client", e);
            }
        }
    }

    @Override
    public O getOffsetContext() {
        return effectiveOffsetContext;
    }

    protected void setEffectiveOffsetContext(O offsetContext) {
        this.effectiveOffsetContext = offsetContext;
    }

    protected void setIgnoreDmlEventByGtidSource(boolean ignoreDmlEventByGtidSource) {
        this.ignoreDmlEventByGtidSource = ignoreDmlEventByGtidSource;
    }

    protected void setGtidChanged(String gtid) {
        metrics.onGtidChange(gtid);
    }

    protected boolean isGtidModeEnabled() {
        return isGtidModeEnabled;
    }

    // todo: perhaps refactor back out to a binary log configurator instance?
    protected BinaryLogClient createBinaryLogClient(BinlogTaskContext<?> taskContext,
                                                    BinlogConnectorConfig connectorConfig,
                                                    Map<String, Thread> clientThreads,
                                                    BinlogConnectorConnection connection) {
        final BinaryLogClient client = taskContext.getBinaryLogClient();
        client.setThreadFactory(
                Threads.threadFactory(
                        getConnectorClass(),
                        connectorConfig.getLogicalName(),
                        "binlog-client",
                        false,
                        false,
                        x -> clientThreads.put(x.getName(), x)));
        client.setServerId(connectorConfig.getServerId());

        client.setSSLMode(sslModeFor(connectorConfig.getSslMode()));
        if (connectorConfig.isSslModeEnabled()) {
            SSLSocketFactory sslSocketFactory = getBinlogSslSocketFactory(connectorConfig, connection);
            if (sslSocketFactory != null) {
                client.setSslSocketFactory(sslSocketFactory);
            }
        }
        client.setUseNonGracefulDisconnect(connectorConfig.usesNonGracefulDisconnect());

        configureReplicaCompatibility(client);

        Configuration configuration = connectorConfig.getConfig();
        final long keepAliveInterval = configuration.getLong(BinlogConnectorConfig.KEEP_ALIVE_INTERVAL_MS);

        client.setKeepAlive(configuration.getBoolean(BinlogConnectorConfig.KEEP_ALIVE));
        client.setKeepAliveInterval(keepAliveInterval);

        // Considering heartbeatInterval should be less than keepAliveInterval, use the heartbeatIntervalFactor
        // multiply by keepAliveInterval and set the result value to heartbeatInterval. The default value of
        // heartbeatIntervalFactor is 0.0, and we believe the left time (0.2 * keepAliveInterval) is enough
        // to process the packet received from the database server.
        client.setHeartbeatInterval((long) (keepAliveInterval * heartbeatIntervalFactor));
        client.setEventDeserializer(createEventDeserializer());
        return client;
    }

    protected void configureReplicaCompatibility(BinaryLogClient client) {
    }

    protected EventDeserializer createEventDeserializer() {
        final Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<>();
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

                    // DBZ-2663 Handle for transaction payload and capture the table map event and add it to the map
                    if (event.getHeader().getEventType() == EventType.TRANSACTION_PAYLOAD) {
                        TransactionPayloadEventData transactionPayloadEventData = event.getData();
                        /**
                         * Loop over the uncompressed events in the transaction payload event and add the table map
                         * event in the map of table events
                         **/
                        for (Event uncompressedEvent : transactionPayloadEventData.getUncompressedEvents()) {
                            if (uncompressedEvent.getHeader().getEventType() == EventType.TABLE_MAP
                                    && uncompressedEvent.getData() != null) {
                                TableMapEventData tableMapEvent = uncompressedEvent.getData();
                                tableMapEventByTableId.put(tableMapEvent.getTableId(), tableMapEvent);
                            }
                        }
                    }

                    // DBZ-5126 Clean cache on rotate event to prevent it from growing indefinitely.
                    if (event.getHeader().getEventType() == EventType.ROTATE && event.getHeader().getTimestamp() != 0) {
                        tableMapEventByTableId.clear();
                    }
                    return event;
                }
                // DBZ-217 In case an event couldn't be read we create a pseudo-event for the sake of logging
                catch (EventDataDeserializationException edde) {
                    // DBZ-3095 As of Java 15, when reaching EOF in the binlog stream, the polling loop in
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

        eventDeserializer.setEventDataDeserializer(EventType.STOP, new StopEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.GTID, new GtidEventDataDeserializer());
        eventDeserializer.setEventDataDeserializer(EventType.WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        eventDeserializer.setEventDataDeserializer(EventType.UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        eventDeserializer.setEventDataDeserializer(EventType.DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_WRITE_ROWS,
                new RowDeserializers.WriteRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_UPDATE_ROWS,
                new RowDeserializers.UpdateRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.EXT_DELETE_ROWS,
                new RowDeserializers.DeleteRowsDeserializer(
                        tableMapEventByTableId, eventDeserializationFailureHandlingMode).setMayContainExtraInformation(true));
        eventDeserializer.setEventDataDeserializer(EventType.TRANSACTION_PAYLOAD,
                new TransactionPayloadDeserializer(tableMapEventByTableId, eventDeserializationFailureHandlingMode));

        return eventDeserializer;
    }

    protected void onEvent(O offsetContext, Event event) {
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
            LOGGER.trace("Received unexpected event with 0 timestamp: {}", event);
            return;
        }

        setEventTimestamp(event, eventTs);

        ts = clock.currentTimeInMillis() - eventTimestamp.toEpochMilli();
        LOGGER.trace("Current milliseconds behind source: {} ms", ts);
        metrics.setMilliSecondsBehindSource(ts);
    }

    protected abstract void setEventTimestamp(Event event, long eventTs);

    protected void ignoreEvent(O offsetContext, Event event) {
        LOGGER.trace("Ignoring event due to missing handler: {}", event);
    }

    protected void handleEvent(P partition, O offsetContext, ChangeEventSourceContext context, Event event) {
        if (event == null) {
            return;
        }

        final EventHeader eventHeader = event.getHeader();
        offsetContext.setBinlogServerId(eventHeader.getServerId());

        final EventType eventType = eventHeader.getEventType();
        if (eventType == EventType.ROTATE) {
            EventData eventData = event.getData();
            RotateEventData rotateEventData;
            if (eventData instanceof EventDeserializer.EventDataWrapper) {
                rotateEventData = (RotateEventData) ((EventDeserializer.EventDataWrapper) eventData).getInternal();
            }
            else {
                rotateEventData = (RotateEventData) eventData;
            }
            offsetContext.setBinlogStartPoint(rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
        }
        else if (eventHeader instanceof EventHeaderV4) {
            EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
            offsetContext.setEventPosition(trackableEventHeader.getPosition(), trackableEventHeader.getEventLength());
        }

        // If there is a handler for this event, forward the event to it ...
        try {
            waitWhenStreamingPaused(context);

            // Forward the event to the handler ...
            eventHandlers.getOrDefault(eventType, (e) -> ignoreEvent(offsetContext, e)).accept(event);

            // Generate heartbeat message if the time is right
            eventDispatcher.dispatchHeartbeatEvent(partition, offsetContext);

            // Capture that we've completed another event ...
            offsetContext.completeEvent();

            // update last offset used for logging
            lastOffset = offsetContext.getOffset();

            if (skipEvent) {
                // We're in the mode of skipping events and we just skipped this one, so decrement our skip count ...
                --initialEventsToSkip;
                skipEvent = initialEventsToSkip > 0;
            }
        }
        catch (RuntimeException e) {
            // There was an error in the event handler, so propagate the failure to Kafka Connect ...
            logStreamingSourceState();
            errorHandler.setProducerThrowable(new DebeziumException("Error processing binlog event", e));
            // Do not stop the client, since Kafka Connect should stop the connector on it's own
            // (and doing it here may cause problems the second time it is stopped).
            // We can clear the listeners though so that we ignore all future events ...
            eventHandlers.clear();
            LOGGER.info(
                    "Error processing binlog event, and propagating to Kafka Connect so it stops this connector. Future binlog events read before connector is shutdown will be ignored.");
        }
        catch (InterruptedException e) {
            // Most likely because this reader was stopped and our thread was interrupted ...
            Thread.currentThread().interrupt();
            eventHandlers.clear();
            LOGGER.info("Stopped processing binlog events due to thread interruption");
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
     * Handle the supplied event that signals that the database has stopped.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerStop(O offsetContext, Event event) {
        LOGGER.debug("Server stopped: {}", event);
    }

    /**
     * Handle the supplied event that is sent by a primary to a replica to let the replica know that
     * the primary is still alive. Not written to a binary log.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerHeartbeat(P partition, O offsetContext, Event event) throws InterruptedException {
        LOGGER.trace("Server heartbeat: {}", event);
        eventDispatcher.dispatchServerHeartbeatEvent(partition, offsetContext);
    }

    /**
     * Handle the supplied event that signals that an out of the ordinary event that occurred on the master.
     * It notifies the replica that something happened on the primary that might cause data to be in an
     * inconsistent state.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerIncident(P partition, O offsetContext, Event event) {
        if (event.getData() instanceof EventDataDeserializationExceptionData) {
            metrics.onErroneousEvent(partition, "source = " + event);
            EventDataDeserializationExceptionData data = event.getData();

            EventHeaderV4 eventHeader = (EventHeaderV4) data.getCause().getEventHeader(); // safe cast, instantiated that ourselves

            // logging some additional context but not the exception itself, this will happen in handleEvent()
            if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                LOGGER.error(
                        "Error while deserializing binlog event at offset {}.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        offsetContext.getOffset(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        offsetContext.getSource().binlogFilename());

                throw new RuntimeException(data.getCause());
            }
            else if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                LOGGER.warn(
                        "Error while deserializing binlog event at offset {}.{}" +
                                "This exception will be ignored and the event be skipped.{}" +
                                "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        offsetContext.getOffset(),
                        System.lineSeparator(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        offsetContext.getSource().binlogFilename(),
                        data.getCause());
            }
        }
        else {
            LOGGER.error("Server incident: {}", event);
        }
    }

    /**
     * Handle the supplied event with a {@link RotateEventData} that signals the logs are being rotated. This
     * means that either the server was restarted, or the binlog has transitioned to a new file. In either case,
     * subsequent table numbers will be different from those seen to this point.
     *
     * @param event the database change data event to be processed; may not be null
     */
    protected void handleRotateLogsEvent(O offsetContext, Event event) {
        LOGGER.debug("Rotating logs: {}", event);
        RotateEventData command = unwrapData(event);
        assert command != null;
        schema.clearTableMappings();
    }

    /**
     * Handle the supplied event with an {@link QueryEventData} by possibly recording the DDL statements as
     * changes in the database schemas.
     *
     * @param partition the partition in which the even occurred
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while recording the DDL statements
     */
    protected void handleQueryEvent(P partition, O offsetContext, Event event) throws InterruptedException {
        Instant eventTime = Conversions.toInstantFromMillis(eventTimestamp.toEpochMilli());
        QueryEventData command = unwrapData(event);
        LOGGER.debug("Received query command: {}", event);
        String sql = command.getSql().trim();
        if (sql.equalsIgnoreCase("BEGIN")) {
            handleTransactionBegin(partition, offsetContext, event, command.getThreadId());
            return;
        }
        if (sql.equalsIgnoreCase("COMMIT")) {
            handleTransactionCompletion(partition, offsetContext, event);
            return;
        }

        String upperCasedStatementBegin = Strings.getBegin(removeSetStatement(sql), 7).toUpperCase();

        if (upperCasedStatementBegin.startsWith("XA ")) {
            // This is an XA transaction, and we currently ignore these and do nothing ...
            return;
        }
        if (schema.ddlFilter().test(sql)) {
            LOGGER.debug("DDL '{}' was filtered out of processing", sql);
            return;
        }
        if (upperCasedStatementBegin.equals("INSERT ") || upperCasedStatementBegin.equals("UPDATE ") || upperCasedStatementBegin.equals("DELETE ")) {
            LOGGER.warn("Received DML '" + sql + "' for processing, binlog probably contains events generated with statement or mixed based replication format");
            return;
        }
        if (sql.equalsIgnoreCase("ROLLBACK")) {
            // We have hit a ROLLBACK which is not supported
            LOGGER.warn("Rollback statements cannot be handled without binlog buffering, the connector will fail. Please check '{}' to see how to enable buffering",
                    BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER.name());
        }

        final List<SchemaChangeEvent> schemaChangeEvents = schema.parseStreamingDdl(partition, sql,
                command.getDatabase(), offsetContext, eventTime);
        try {
            for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
                if (schema.skipSchemaChangeEvent(schemaChangeEvent)) {
                    continue;
                }

                final TableId tableId = schemaChangeEvent.getTables().isEmpty() ? null : schemaChangeEvent.getTables().iterator().next().id();
                if (tableId != null && !connectorConfig.getSkippedOperations().contains(Envelope.Operation.TRUNCATE)
                        && schemaChangeEvent.getType().equals(SchemaChangeEvent.SchemaChangeEventType.TRUNCATE)) {
                    eventDispatcher.dispatchDataChangeEvent(partition, tableId,
                            new BinlogChangeRecordEmitter<>(partition, offsetContext, clock, Envelope.Operation.TRUNCATE, null, null, connectorConfig));
                }
                eventDispatcher.dispatchSchemaChangeEvent(partition, offsetContext, tableId, (receiver) -> {
                    try {
                        receiver.schemaChangeEvent(schemaChangeEvent);
                    }
                    catch (Exception e) {
                        throw new DebeziumException(e);
                    }
                });
            }
        }
        catch (InterruptedException e) {
            LOGGER.info("Processing interrupted");
        }
    }

    private String removeSetStatement(String sql) {
        return sql.replaceAll(SET_STATEMENT_REGEX, "").trim();
    }

    /**
     * Handle a change in the table metadata.<p></p>
     *
     * This method should be called whenever we consume a TABLE_MAP event, and every transaction in the log should
     * include one of these for each table affected by the transaction. Each table map event includes a
     * monotonically-increasing numeric identifier, and this identifier is used within subsequent events within
     * the same transaction. This table identifier can change when:
     * <ol>
     * <li>the table structure is modified (e.g., via an {@code ALTER TABLE ...} command); or</li>
     * <li>MySQL rotates to a new binary log file, even if the table structure does not change.</li>
     * </ol>
     *
     * @param partition the partition; never null
     * @param offsetContext the offset context; never null
     * @param event the update event; never null
     */
    protected void handleUpdateTableMetadata(P partition, O offsetContext, Event event) throws InterruptedException {
        TableMapEventData metadata = unwrapData(event);
        long tableNumber = metadata.getTableId();
        String databaseName = metadata.getDatabase();
        String tableName = metadata.getTable();
        TableId tableId = new TableId(databaseName, null, tableName);
        if (schema.assignTableNumber(tableNumber, tableId)) {
            LOGGER.debug("Received update table metadata event: {}", event);
        }
        else {
            informAboutUnknownTableIfRequired(partition, offsetContext, event, tableId);
        }
    }

    /**
     * Handle an event of type TRANSACTION_PAYLOAD_EVENT<p></p>
     *
     * This method should be called whenever a transaction payload event is encountered by the mysql binlog connector.
     * A Transaction payload event is propagated from the binlog connector when compression is turned on over binlog.
     * This method loops over the individual events in the compressed binlog and calls the respective atomic event
     * handlers.
     *
     * @param partition the partition; never null
     * @param offsetContext the offset context; never null
     * @param event the transaction payload event; never null
     */
    protected void handleTransactionPayload(P partition, O offsetContext, Event event) throws InterruptedException {
        TransactionPayloadEventData transactionPayloadEventData = event.getData();
        // Loop over the uncompressed events in the transaction payload event and add the table map
        // event in the map of table events
        for (Event uncompressedEvent : transactionPayloadEventData.getUncompressedEvents()) {
            eventHandlers.getOrDefault(
                    uncompressedEvent.getHeader().getEventType(),
                    (e) -> ignoreEvent(offsetContext, uncompressedEvent)).accept(uncompressedEvent);
        }
    }

    /**
     * Generate source records for the supplied event with an {@link WriteRowsEventData}.
     *
     * @param partition the partition in which the even occurred
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleInsert(P partition, O offsetContext, Event event) throws InterruptedException {
        handleChange(partition, offsetContext, event, Envelope.Operation.CREATE, WriteRowsEventData.class,
                x -> schema.getTableId(x.getTableId()),
                WriteRowsEventData::getRows,
                (tableId, row) -> eventDispatcher.dispatchDataChangeEvent(partition, tableId,
                        new BinlogChangeRecordEmitter<>(partition, offsetContext, clock, Envelope.Operation.CREATE, null, row, connectorConfig)),
                (tableId, row) -> validateChangeEventWithTable(taskContext.getSchema().tableFor(tableId), null, row));
    }

    /**
     * Generate source records for the supplied event with an {@link UpdateRowsEventData}.
     *
     * @param partition the partition in which the even occurred
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleUpdate(P partition, O offsetContext, Event event) throws InterruptedException {
        handleChange(partition, offsetContext, event, Envelope.Operation.UPDATE, UpdateRowsEventData.class,
                x -> schema.getTableId(x.getTableId()),
                UpdateRowsEventData::getRows,
                (tableId, row) -> eventDispatcher.dispatchDataChangeEvent(partition, tableId,
                        new BinlogChangeRecordEmitter<>(partition, offsetContext, clock, Envelope.Operation.UPDATE, row.getKey(), row.getValue(),
                                connectorConfig)),
                (tableId, row) -> validateChangeEventWithTable(taskContext.getSchema().tableFor(tableId), row.getKey(), row.getValue()));
    }

    /**
     * Generate source records for the supplied event with an {@link DeleteRowsEventData}.
     *
     * @param partition the partition in which the even occurred
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleDelete(P partition, O offsetContext, Event event) throws InterruptedException {
        handleChange(partition, offsetContext, event, Envelope.Operation.DELETE, DeleteRowsEventData.class,
                x -> schema.getTableId(x.getTableId()),
                DeleteRowsEventData::getRows,
                (tableId, row) -> eventDispatcher.dispatchDataChangeEvent(partition, tableId,
                        new BinlogChangeRecordEmitter<>(partition, offsetContext, clock, Envelope.Operation.DELETE, row, null, connectorConfig)),
                (tableId, row) -> validateChangeEventWithTable(taskContext.getSchema().tableFor(tableId), row, null));
    }

    /**
     * Handle a {@link EventType#VIEW_CHANGE} event.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void viewChange(O offsetContext, Event event) throws InterruptedException {
        LOGGER.debug("View Change event: {}", event);
        // do nothing
    }

    /**
     * Handle a {@link EventType#XA_PREPARE} event.
     *
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void prepareTransaction(O offsetContext, Event event) throws InterruptedException {
        LOGGER.debug("XA Prepare event: {}", event);
        // do nothing
    }

    protected void handleTransactionBegin(P partition, O offsetContext, Event event, Long threadId) throws InterruptedException {
        Instant eventTime = Conversions.toInstantFromMillis(eventTimestamp.toEpochMilli());
        // We are starting a new transaction ...
        offsetContext.startNextTransaction();
        eventDispatcher.dispatchTransactionStartedEvent(partition, offsetContext.getTransactionId(), offsetContext, eventTime);
        if (threadId != null) {
            offsetContext.setBinlogThread(threadId);
        }
        if (initialEventsToSkip != 0) {
            LOGGER.debug("Restarting partially-processed transaction; change events will not be created for the first {} events plus {} more rows in the next event",
                    initialEventsToSkip, startingRowNumber);
            // We are restarting, so we need to skip the events in this transaction that we processed previously...
            skipEvent = true;
        }
    }

    /**
     * @return the connector class type
     */
    protected abstract Class<? extends SourceConnector> getConnectorClass();

    /**
     * Handle the GTID event.
     *
     * @param partition the partition; never null
     * @param offsetContext the offset context; never null
     * @param event the event
     * @param gtidSourceFilter the GTID source filter
     * @throws InterruptedException if the thread is interrupted
     */
    protected abstract void handleGtidEvent(P partition, O offsetContext, Event event, Predicate<String> gtidSourceFilter) throws InterruptedException;

    /**
     * Handle the supplied event with a {@link RowsQueryEventData} or {@link AnnotateRowsEventData}
     * by recording the original SQL query that generated the event.
     *
     * @param offsetContext the offset context; never null
     * @param event the database change data event to be processed; may not be null
     */
    protected abstract void handleRecordingQuery(O offsetContext, Event event);

    /**
     * @return the event type for reading query events from
     */
    protected abstract EventType getIncludeQueryEventType();

    /**
     * @return the event type for the global transaction identifier events
     */
    protected abstract EventType getGtidEventType();

    /**
     * Initialize the connector's global transaction identifier set.
     *
     * @param value the value to initialize the set based upon
     */
    protected abstract void initializeGtidSet(String value);

    private Predicate<String> getGtidDmlSourceFilter() {
        if (connectorConfig.getConfig().getBoolean(BinlogConnectorConfig.GTID_SOURCE_FILTER_DML_EVENTS)) {
            return connectorConfig.getGtidSourceFilter();
        }
        return null;
    }

    private void handleTransactionCompletion(P partition, O offsetContext, Event event) throws InterruptedException {
        // We are completing the transaction ...
        eventDispatcher.dispatchTransactionCommittedEvent(
                partition,
                offsetContext,
                Conversions.toInstantFromMillis(eventTimestamp.toEpochMilli()));
        offsetContext.commitTransaction();
        offsetContext.setBinlogThread(-1L);
        skipEvent = false;
        ignoreDmlEventByGtidSource = false;
    }

    /**
     * If we receive an event for a table that is monitored but whose metadata we don't know, either ignore
     * that event or raise a warning or error per the {@link BinlogConnectorConfig#INCONSISTENT_SCHEMA_HANDLING_MODE}
     * configuration.
     *
     * @param partition the partition; never null
     * @param offsetContext the offset context; never null
     * @param event the binlog event
     * @param tableId the table identifier
     * @param operation the operation being performed
     */
    private void informAboutUnknownTableIfRequired(P partition, O offsetContext, Event event, TableId tableId,
                                                   Envelope.Operation operation)
            throws InterruptedException {
        if (tableId != null && connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            metrics.onErroneousEvent(partition, "source = " + tableId + ", event " + event, operation);
            EventHeaderV4 eventHeader = event.getHeader();

            if (inconsistentSchemaHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                LOGGER.error(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database schema history topic. Take a new snapshot in this case.{}"
                                + "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event, offsetContext.getOffset(), tableId, System.lineSeparator(), eventHeader.getPosition(),
                        eventHeader.getNextPosition(), offsetContext.getSource().binlogFilename());
                throw new DebeziumException("Encountered change event for table " + tableId
                        + " whose schema isn't known to this connector");
            }
            else if (inconsistentSchemaHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                LOGGER.warn(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database schema history topic. Take a new snapshot in this case.{}"
                                + "The event will be ignored.{}"
                                + "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event, offsetContext.getOffset(), tableId, System.lineSeparator(), System.lineSeparator(),
                        eventHeader.getPosition(), eventHeader.getNextPosition(), offsetContext.getSource().binlogFilename());
            }
            else {
                LOGGER.debug(
                        "Encountered change event '{}' at offset {} for table {} whose schema isn't known to this connector. One possible cause is an incomplete database schema history topic. Take a new snapshot in this case.{}"
                                + "The event will be ignored.{}"
                                + "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        event, offsetContext.getOffset(), tableId, System.lineSeparator(), System.lineSeparator(),
                        eventHeader.getPosition(), eventHeader.getNextPosition(), offsetContext.getSource().binlogFilename());
            }
        }
        else {
            if (tableId == null) {
                EventData eventData = unwrapData(event);
                if (eventData instanceof WriteRowsEventData) {
                    tableId = schema.getExcludeTableId(((WriteRowsEventData) eventData).getTableId());
                }
                else if (eventData instanceof UpdateRowsEventData) {
                    tableId = schema.getExcludeTableId(((UpdateRowsEventData) eventData).getTableId());
                }
                else if (eventData instanceof DeleteRowsEventData) {
                    tableId = schema.getExcludeTableId(((DeleteRowsEventData) eventData).getTableId());
                }
            }
            LOGGER.trace("Filtered {} event for {}", event.getHeader().getEventType(), tableId);
            metrics.onFilteredEvent(partition, "source = " + tableId, operation);
            eventDispatcher.dispatchFilteredEvent(partition, offsetContext);
        }
    }

    /**
     * If we receive an event for a table that is monitored but whose metadata we don't know, either ignore
     * that event or raise a warning or error per the {@link BinlogConnectorConfig#INCONSISTENT_SCHEMA_HANDLING_MODE}
     * configuration.
     *
     * @param partition the partition; never null
     * @param offsetContext the offset context; never null
     * @param event the binlog event
     * @param tableId the table identifier
     * @throws InterruptedException if the thread is interrupted
     */
    private void informAboutUnknownTableIfRequired(P partition, O offsetContext, Event event, TableId tableId)
            throws InterruptedException {
        informAboutUnknownTableIfRequired(partition, offsetContext, event, tableId, null);
    }

    private void validateChangeEventWithTable(Table table, Object[] before, Object[] after) {
        if (table != null) {
            int columnSize = table.columns().size();
            String message = "Error processing {} of row in {} because it's different column size with internal schema size {}, but {} size {}, " +
                    "restart connector with schema recovery mode.";
            if (before != null && columnSize != before.length) {
                LOGGER.error(message, "before", table.id().table(), columnSize, "before", before.length);
                throw new DebeziumException(
                        "Error processing row in " + table.id().table() + ", internal schema size " + columnSize + ", but row size " + before.length + " , " +
                                "restart connector with schema recovery mode.");
            }
            if (after != null && columnSize != after.length) {
                LOGGER.error(message, "after", table.id().table(), columnSize, "after", after.length);
                throw new DebeziumException(
                        "Error processing row in " + table.id().table() + ", internal schema size " + columnSize + ", but row size " + after.length + " , " +
                                "restart connector with schema recovery mode.");
            }
        }
    }

    private <T extends EventData, U> void handleChange(P partition,
                                                       O offsetContext,
                                                       Event event,
                                                       Envelope.Operation operation,
                                                       Class<T> eventDataClass,
                                                       TableIdProvider<T> tableIdProvider,
                                                       RowsProvider<T, U> rowsProvider,
                                                       BinlogChangeEmitter<U> changeEmitter,
                                                       ChangeEventValidator<U> changeEventValidator)
            throws InterruptedException {
        if (skipEvent) {
            // We can skip this because we should already be at least this far ...
            LOGGER.info("Skipping previously processed row event: {}", event);
            return;
        }
        if (ignoreDmlEventByGtidSource) {
            LOGGER.debug("Skipping DML event because this GTID source is filtered: {}", event);
            return;
        }
        final T data = unwrapData(event);
        final TableId tableId = tableIdProvider.getTableId(data);
        final List<U> rows = rowsProvider.getRows(data);
        String changeType = operation.name();

        if (tableId != null && taskContext.getSchema().schemaFor(tableId) != null) {
            int count = 0;
            int numRows = rows.size();
            if (startingRowNumber < numRows) {
                for (int rowIndex = startingRowNumber; rowIndex != numRows; ++rowIndex) {
                    U row = rows.get(rowIndex);
                    changeEventValidator.validate(tableId, row);
                    offsetContext.setRowNumber(rowIndex, numRows);
                    offsetContext.event(tableId, eventTimestamp);
                    changeEmitter.emit(tableId, row);
                    count++;
                }
                if (LOGGER.isDebugEnabled()) {
                    if (startingRowNumber != 0) {
                        LOGGER.debug("Emitted {} {} record(s) for last {} row(s) in event: {}",
                                count, changeType, numRows - startingRowNumber, event);
                    }
                    else {
                        LOGGER.debug("Emitted {} {} record(s) for event: {}", count, changeType, event);
                    }
                }
                offsetContext.changeEventCompleted();
            }
            else {
                // All rows were previously processed ...
                LOGGER.debug("Skipping previously processed {} event: {}", changeType, event);
            }
        }
        else {
            informAboutUnknownTableIfRequired(partition, offsetContext, event, tableId, operation);
        }
        startingRowNumber = 0;
    }

    private void waitWhenStreamingPaused(ChangeEventSourceContext context) throws InterruptedException {
        if (context.isPaused()) {
            LOGGER.info("Streaming will now pause");
            context.streamingPaused();
            context.waitSnapshotCompletion();
            LOGGER.info("Streaming resumed");
        }
    }

    private void logStreamingSourceState() {
        logStreamingSourceState(Level.ERROR);
    }

    protected void logEvent(O offsetContext, Event event) {
        LOGGER.trace("Received event: {}", event);
    }

    private void logStreamingSourceState(Level severity) {
        final Object position = client == null ? "N/A" : client.getBinlogFilename() + "/" + client.getBinlogPosition();
        final String message = "Error during binlog processing. Last offset stored = {}, binlog reader near position = {}";
        switch (severity) {
            case WARN:
                LOGGER.warn(message, lastOffset, position);
                break;
            case DEBUG:
                LOGGER.debug(message, lastOffset, position);
                break;
            default:
                LOGGER.error(message, lastOffset, position);
        }
    }

    BinlogStreamingChangeEventSourceMetrics<?, P> getMetrics() {
        return metrics;
    }

    void rewindBinaryLogClient(ChangeEventSourceContext context, BinlogPosition position) {
        try {
            if (context.isRunning()) {
                LOGGER.debug("Rewinding binlog to position {}", position);
                client.disconnect();
                client.setBinlogFilename(position.getFilename());
                client.setBinlogPosition(position.getPosition());
                client.connect();
            }
        }
        catch (IOException e) {
            LOGGER.error("Unexpected error when re-connecting to the MySQL binary log reader", e);
        }
    }

    BinlogPosition getCurrentBinlogPosition() {
        return new BinlogPosition(client.getBinlogFilename(), client.getBinlogPosition());
    }

    /**
     * Wraps the specified exception in a {@link DebeziumException}, ensuring that all useful state is captured inside
     * the new exception's message.
     *
     * @param error the exception; may not be null
     * @return the wrapped Kafka Connect exception
     */
    protected DebeziumException wrap(Throwable error) {
        assert error != null;
        String msg = error.getMessage();
        if (error instanceof ServerException) {
            ServerException e = (ServerException) error;
            msg = msg + " Error code: " + e.getErrorCode() + "; SQLSTATE: " + e.getSqlState() + ".";
        }
        else if (error instanceof SQLException) {
            SQLException e = (SQLException) error;
            msg = e.getMessage() + " Error code: " + e.getErrorCode() + "; SQLSTATE: " + e.getSQLState() + ".";
        }
        return new DebeziumException(msg, error);
    }

    protected final class ReaderThreadLifecycleListener implements BinaryLogClient.LifecycleListener {
        private final O offsetContext;

        ReaderThreadLifecycleListener(O offsetContext) {
            this.offsetContext = offsetContext;
        }

        @Override
        public void onDisconnect(BinaryLogClient client) {
            if (LOGGER.isInfoEnabled()) {
                taskContext.temporaryLoggingContext(connectorConfig, "binlog", () -> {
                    Map<String, ?> offset = lastOffset;
                    if (offset != null) {
                        LOGGER.info("Stopped reading binlog after {} events, last recorded offset: {}", totalRecordCounter, offset);
                    }
                    else {
                        LOGGER.info("Stopped reading binlog after {} events, no new offset was recorded", totalRecordCounter);
                    }
                });
            }
        }

        @Override
        public void onConnect(BinaryLogClient client) {
            // Set up the MDC logging context for this thread ...
            taskContext.configureLoggingContext("binlog");
            // The event row number will be used when processing the first event ...
            LOGGER.info("Connected to binlog at {}:{}, starting at {}",
                    connectorConfig.getHostName(), connectorConfig.getPort(), offsetContext);
        }

        @Override
        public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
            LOGGER.debug("A communication failure event arrived", ex);
            logStreamingSourceState();
            try {
                // Stop BinaryLogClient background threads
                client.disconnect();
            }
            catch (final Exception e) {
                LOGGER.debug("Exception while closing client", e);
            }
            errorHandler.setProducerThrowable(wrap(ex));
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
            if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.FAIL) {
                LOGGER.debug("A deserialization failure event arrived", ex);
                logStreamingSourceState();
                errorHandler.setProducerThrowable(wrap(ex));
            }
            else if (eventDeserializationFailureHandlingMode == EventProcessingFailureHandlingMode.WARN) {
                LOGGER.warn("A deserialization failure event arrived", ex);
                logStreamingSourceState(Level.WARN);
            }
            else {
                LOGGER.debug("A deserialization failure event arrived", ex);
                logStreamingSourceState(Level.DEBUG);
            }
        }
    }

    private SSLMode sslModeFor(SecureConnectionMode mode) {
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

    private SSLSocketFactory getBinlogSslSocketFactory(BinlogConnectorConfig connectorConfig, BinlogConnectorConnection connection) {
        String acceptedTlsVersion = connection.getSessionVariableForSslVersion();
        if (!isNullOrEmpty(acceptedTlsVersion)) {
            SSLMode sslMode = sslModeFor(connectorConfig.getSslMode());
            LOGGER.info("Enable ssl " + sslMode + " mode for connector " + connectorConfig.getLogicalName());

            final char[] keyPasswordArray = connection.connectionConfig().sslKeyStorePassword();
            final String keyFilename = connection.connectionConfig().sslKeyStore();
            final char[] trustPasswordArray = connection.connectionConfig().sslTrustStorePassword();
            final String trustFilename = connection.connectionConfig().sslTrustStore();
            KeyManager[] keyManagers = null;
            if (keyFilename != null) {
                try {
                    KeyStore ks = connection.loadKeyStore(keyFilename, keyPasswordArray);

                    KeyManagerFactory kmf = KeyManagerFactory.getInstance("NewSunX509");
                    kmf.init(ks, keyPasswordArray);

                    keyManagers = kmf.getKeyManagers();
                }
                catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
                    throw new DebeziumException("Could not load keystore", e);
                }
            }
            TrustManager[] trustManagers;
            try {
                KeyStore ks = null;
                if (trustFilename != null) {
                    ks = connection.loadKeyStore(trustFilename, trustPasswordArray);
                }

                if (ks == null && (sslMode == SSLMode.PREFERRED || sslMode == SSLMode.REQUIRED)) {
                    trustManagers = new TrustManager[]{
                            new X509TrustManager() {
                                @Override
                                public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                                        throws CertificateException {
                                }

                                @Override
                                public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                                        throws CertificateException {
                                }

                                @Override
                                public X509Certificate[] getAcceptedIssuers() {
                                    return new X509Certificate[0];
                                }
                            }
                    };
                }
                else {
                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(ks);
                    trustManagers = tmf.getTrustManagers();
                }
            }
            catch (KeyStoreException | NoSuchAlgorithmException e) {
                throw new DebeziumException("Could not load truststore", e);
            }

            // DBZ-1208 Resembles the logic from the upstream BinaryLogClient, only that
            // the accepted TLS version is passed to the constructed factory
            final KeyManager[] finalKMS = keyManagers;
            return new DefaultSSLSocketFactory(acceptedTlsVersion) {
                @Override
                protected void initSSLContext(SSLContext sc) throws GeneralSecurityException {
                    sc.init(finalKMS, trustManagers, null);
                }
            };
        }
        return null;
    }

    @FunctionalInterface
    private interface BinlogChangeEmitter<T> {
        void emit(TableId tableId, T data) throws InterruptedException;
    }

    @FunctionalInterface
    private interface TableIdProvider<E extends EventData> {
        TableId getTableId(E data);
    }

    @FunctionalInterface
    private interface RowsProvider<E extends EventData, U> {
        List<U> getRows(E data);
    }

    @FunctionalInterface
    private interface ChangeEventValidator<U> {
        void validate(TableId tableId, U row);
    }

    /**
     * Class that represents a binlog file and position
     */
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
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

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
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializationException;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import com.github.shyiko.mysql.binlog.network.SSLMode;

import io.debezium.connector.mysql.MySqlConnectorConfig.EventDeserializationFailureHandlingMode;
import io.debezium.connector.mysql.MySqlConnectorConfig.SecureConnectionMode;
import io.debezium.connector.mysql.RecordMakers.RecordsForTable;
import io.debezium.function.BlockingConsumer;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Strings;

/**
 * A component that reads the binlog of a MySQL server, and records any schema changes in {@link MySqlSchema}.
 *
 * @author Randall Hauch
 *
 */
public class BinlogReader extends AbstractReader {

    private static final long INITIAL_POLL_PERIOD_IN_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private static final long MAX_POLL_PERIOD_IN_MILLIS = TimeUnit.HOURS.toMillis(1);

    private final boolean recordSchemaChangesInSourceRecords;
    private final RecordMakers recordMakers;
    private final SourceInfo source;
    private final EnumMap<EventType, BlockingConsumer<Event>> eventHandlers = new EnumMap<>(EventType.class);
    private final BinaryLogClient client;
    private final BinlogReaderMetrics metrics;
    private final Clock clock;
    private final ElapsedTimeStrategy pollOutputDelay;
    private final EventDeserializationFailureHandlingMode eventDeserializationFailureHandlingMode;

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
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            BinlogPosition other = (BinlogPosition) obj;
            if (!filename.equals(other.filename))
                return false;
            if (position != other.position)
                return false;
            return true;
        }
    }

    /**
     * Create a binlog reader.
     *
     * @param name the name of this reader; may not be null
     * @param context the task context in which this reader is running; may not be null
     */
    public BinlogReader(String name, MySqlTaskContext context) {
        super(name, context);

        source = context.source();
        recordMakers = context.makeRecord();
        recordSchemaChangesInSourceRecords = context.includeSchemaChangeRecords();
        clock = context.clock();
        eventDeserializationFailureHandlingMode = context.eventDeserializationFailureHandlingMode();

        // Use exponential delay to log the progress frequently at first, but the quickly tapering off to once an hour...
        pollOutputDelay = ElapsedTimeStrategy.exponential(clock, INITIAL_POLL_PERIOD_IN_MILLIS, MAX_POLL_PERIOD_IN_MILLIS);

        // Set up the log reader ...
        client = new BinaryLogClient(context.hostname(), context.port(), context.username(), context.password());
        client.setServerId(context.serverId());
        client.setSSLMode(sslModeFor(context.sslMode()));
        client.setKeepAlive(context.config().getBoolean(MySqlConnectorConfig.KEEP_ALIVE));
        client.registerEventListener(context.bufferSizeForBinlogReader() == 0
                ? this::handleEvent
                : (new EventBuffer(context.bufferSizeForBinlogReader(), this))::add);

        client.registerLifecycleListener(new ReaderThreadLifecycleListener());
        if (logger.isDebugEnabled()) client.registerEventListener(this::logEvent);

        boolean filterDmlEventsByGtidSource = context.config().getBoolean(MySqlConnectorConfig.GTID_SOURCE_FILTER_DML_EVENTS);
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
                catch(EventDataDeserializationException edde) {
                    EventHeaderV4 header = new EventHeaderV4();
                    header.setEventType(EventType.INCIDENT);
                    header.setTimestamp(edde.getEventHeader().getTimestamp());
                    header.setServerId(edde.getEventHeader().getServerId());

                    if(edde.getEventHeader() instanceof EventHeaderV4) {
                        header.setEventLength(((EventHeaderV4)edde.getEventHeader()).getEventLength());
                        header.setNextPosition(((EventHeaderV4)edde.getEventHeader()).getNextPosition());
                        header.setFlags(((EventHeaderV4)edde.getEventHeader()).getFlags());
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
        metrics = new BinlogReaderMetrics(client);
    }

    @Override
    protected void doInitialize() {
        metrics.register(context, logger);
    }

    @Override
    protected void doStart() {
        // Register our event handlers ...
        eventHandlers.put(EventType.STOP, this::handleServerStop);
        eventHandlers.put(EventType.HEARTBEAT, this::handleServerHeartbeat);
        eventHandlers.put(EventType.INCIDENT, this::handleServerIncident);
        eventHandlers.put(EventType.ROTATE, this::handleRotateLogsEvent);
        eventHandlers.put(EventType.TABLE_MAP, this::handleUpdateTableMetadata);
        eventHandlers.put(EventType.QUERY, this::handleQueryEvent);
        eventHandlers.put(EventType.WRITE_ROWS, this::handleInsert);
        eventHandlers.put(EventType.UPDATE_ROWS, this::handleUpdate);
        eventHandlers.put(EventType.DELETE_ROWS, this::handleDelete);
        eventHandlers.put(EventType.EXT_WRITE_ROWS, this::handleInsert);
        eventHandlers.put(EventType.EXT_UPDATE_ROWS, this::handleUpdate);
        eventHandlers.put(EventType.EXT_DELETE_ROWS, this::handleDelete);
        eventHandlers.put(EventType.VIEW_CHANGE, this::viewChange);
        eventHandlers.put(EventType.XA_PREPARE, this::prepareTransaction);

        // Get the current GtidSet from MySQL so we can get a filtered/merged GtidSet based off of the last Debezium checkpoint.
        String availableServerGtidStr = context.knownGtidSet();
        if (availableServerGtidStr != null && !availableServerGtidStr.trim().isEmpty()) {
            // The server is using GTIDs, so enable the handler ...
            eventHandlers.put(EventType.GTID, this::handleGtidEvent);

            // Now look at the GTID set from the server and what we've previously seen ...
            GtidSet availableServerGtidSet = new GtidSet(availableServerGtidStr);
            GtidSet filteredGtidSet = context.filterGtidSet(availableServerGtidSet);
            if (filteredGtidSet != null) {
                // We've seen at least some GTIDs, so start reading from the filtered GTID set ...
                logger.info("Registering binlog reader with GTID set: {}", filteredGtidSet);
                String filteredGtidSetStr = filteredGtidSet.toString();
                client.setGtidSet(filteredGtidSetStr);
                source.setCompletedGtidSet(filteredGtidSetStr);
                gtidSet = new com.github.shyiko.mysql.binlog.GtidSet(filteredGtidSetStr);
            } else {
                // We've not yet seen any GTIDs, so that means we have to start reading the binlog from the beginning ...
                client.setBinlogFilename(source.binlogFilename());
                client.setBinlogPosition(source.binlogPosition());
                gtidSet = new com.github.shyiko.mysql.binlog.GtidSet("");
            }
        } else {
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
            long timeoutInMilliseconds = context.timeoutInMilliseconds();
            long started = context.clock().currentTimeInMillis();
            try {
                logger.debug("Attempting to establish binlog reader connection with timeout of {} ms", timeoutInMilliseconds);
                client.connect(context.timeoutInMilliseconds());
            } catch (TimeoutException e) {
                // If the client thread is interrupted *before* the client could connect, the client throws a timeout exception
                // The only way we can distinguish this is if we get the timeout exception before the specified timeout has
                // elapsed, so we simply check this (within 10%) ...
                long duration = context.clock().currentTimeInMillis() - started;
                if (duration > (0.9 * context.timeoutInMilliseconds())) {
                    double actualSeconds = TimeUnit.MILLISECONDS.toSeconds(duration);
                    throw new ConnectException("Timed out after " + actualSeconds + " seconds while waiting to connect to MySQL at " +
                            context.hostname() + ":" + context.port() + " with user '" + context.username() + "'", e);
                }
                // Otherwise, we were told to shutdown, so we don't care about the timeout exception
            } catch (AuthenticationException e) {
                throw new ConnectException("Failed to authenticate to the MySQL database at " +
                        context.hostname() + ":" + context.port() + " with user '" + context.username() + "'", e);
            } catch (Throwable e) {
                throw new ConnectException("Unable to connect to the MySQL database at " +
                        context.hostname() + ":" + context.port() + " with user '" + context.username() + "': " + e.getMessage(), e);
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
        } catch (IOException e) {
            logger.error("Unexpected error when re-connecting to the MySQL binary log reader", e);
        }
    }

    @Override
    protected void doStop() {
        try {
            if (isRunning()) {
                logger.debug("Stopping binlog reader, last recorded offset: {}", lastOffset);
                client.disconnect();
            }
            cleanupResources();
        } catch (IOException e) {
            logger.error("Unexpected error when disconnecting from the MySQL binary log reader", e);
        } finally {
            // We unregister our JMX metrics now, which means we won't record metrics for records that
            // may be processed between now and complete shutdown. That's okay.
            metrics.unregister(logger);
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
                    context.temporaryLoggingContext("binlog", () -> {
                        logger.info("{} records sent during previous {}, last recorded offset: {}",
                                    recordCounter, Strings.duration(millisSinceLastOutput), lastOffset);
                    });
                } finally {
                    recordCounter = 0;
                    previousOutputMillis += millisSinceLastOutput;
                }
            }
        }
    }

    protected void logEvent(Event event) {
        logger.trace("Received event: {}", event);
    }

    protected void ignoreEvent(Event event) {
        logger.trace("Ignoring event due to missing handler: {}", event);
    }

    protected void handleEvent(Event event) {
        if (event == null) return;

        // Update the source offset info. Note that the client returns the value in *milliseconds*, even though the binlog
        // contains only *seconds* precision ...
        EventHeader eventHeader = event.getHeader();
        source.setBinlogTimestampSeconds(eventHeader.getTimestamp() / 1000L); // client returns milliseconds, but only second
                                                                              // precision
        source.setBinlogServerId(eventHeader.getServerId());
        EventType eventType = eventHeader.getEventType();
        if (eventType == EventType.ROTATE) {
            EventData eventData = event.getData();
            RotateEventData rotateEventData;
            if (eventData instanceof EventDeserializer.EventDataWrapper) {
                rotateEventData = (RotateEventData) ((EventDeserializer.EventDataWrapper) eventData).getInternal();
            } else {
                rotateEventData = (RotateEventData) eventData;
            }
            source.setBinlogStartPoint(rotateEventData.getBinlogFilename(), rotateEventData.getBinlogPosition());
        } else if (eventHeader instanceof EventHeaderV4) {
            EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
            source.setEventPosition(trackableEventHeader.getPosition(), trackableEventHeader.getEventLength());
        }

        // If there is a handler for this event, forward the event to it ...
        try {
            // Forward the event to the handler ...
            eventHandlers.getOrDefault(eventType, this::ignoreEvent).accept(event);

            // Capture that we've completed another event ...
            source.completeEvent();

            if (skipEvent) {
                // We're in the mode of skipping events and we just skipped this one, so decrement our skip count ...
                --initialEventsToSkip;
                skipEvent = initialEventsToSkip > 0;
            }

        } catch (RuntimeException e) {
            // There was an error in the event handler, so propagate the failure to Kafka Connect ...
            logReaderState();
            failed(e, "Error processing binlog event");
            // Do not stop the client, since Kafka Connect should stop the connector on it's own
            // (and doing it here may cause problems the second time it is stopped).
            // We can clear the listeners though so that we ignore all future events ...
            eventHandlers.clear();
            logger.info("Error processing binlog event, and propagating to Kafka Connect so it stops this connector. Future binlog events read before connector is shutdown will be ignored.");
        } catch (InterruptedException e) {
            // Most likely because this reader was stopped and our thread was interrupted ...
            Thread.interrupted();
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
     * Handle the supplied event that is sent by a master to a slave to let the slave know that the master is still alive. Not
     * written to a binary log.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerHeartbeat(Event event) {
        logger.trace("Server heartbeat: {}", event);
    }

    /**
     * Handle the supplied event that signals that an out of the ordinary event that occurred on the master. It notifies the slave
     * that something happened on the master that might cause data to be in an inconsistent state.
     *
     * @param event the server stopped event to be processed; may not be null
     */
    protected void handleServerIncident(Event event) {
        if (event.getData() instanceof EventDataDeserializationExceptionData) {
            EventDataDeserializationExceptionData data = event.getData();

            EventHeaderV4 eventHeader = (EventHeaderV4) data.getCause().getEventHeader(); // safe cast, instantiated that ourselves

            // logging some additional context but not the exception itself, this will happen in handleEvent()
            if(eventDeserializationFailureHandlingMode == EventDeserializationFailureHandlingMode.FAIL) {
                logger.error(
                        "Error while deserializing binlog event at offset {}.{}" +
                        "Use the mysqlbinlog tool to view the problematic event: mysqlbinlog --start-position={} --stop-position={} --verbose {}",
                        source.offset(),
                        System.lineSeparator(),
                        eventHeader.getPosition(),
                        eventHeader.getNextPosition(),
                        source.binlogFilename()
                );

                throw new RuntimeException(data.getCause());
            }
            else if(eventDeserializationFailureHandlingMode == EventDeserializationFailureHandlingMode.WARN) {
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
                        data.getCause()
                );
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
            // We are completing the transaction ...
            source.commitTransaction();
            source.setBinlogThread(-1L);
            skipEvent = false;
            ignoreDmlEventByGtidSource = false;
            return;
        }
        if (sql.toUpperCase().startsWith("XA ")) {
            // This is an XA transaction, and we currently ignore these and do nothing ...
            return;
        }
        if (context.ddlFilter().test(sql)) {
            logger.debug("DDL '{}' was filtered out of processing", sql);
            return;
        }
        if (sql.equalsIgnoreCase("ROLLBACK")) {
            // We have hit a ROLLBACK which is not supported
            logger.warn("Rollback statements cannot be handled without binlog buffering, the connector will fail. Please check '{}' to see how to enable buffering",
                    MySqlConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER.name());
        }
        context.dbSchema().applyDdl(context.source(), command.getDatabase(), command.getSql(), (dbName, statements) -> {
            if (recordSchemaChangesInSourceRecords && recordMakers.schemaChanges(dbName, statements, super::enqueueRecord) > 0) {
                logger.debug("Recorded DDL statements for database '{}': {}", dbName, statements);
            }
        });
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
        } else {
            logger.debug("Skipping update table metadata event: {}", event);
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
            Long ts = context.clock().currentTimeInMillis();
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
                    } else {
                        logger.debug("Recorded {} insert record(s) for event: {}", count, event);
                    }
                }
            } else {
                // All rows were previously processed ...
                logger.debug("Skipping previously processed insert event: {}", event);
            }
        } else {
            logger.debug("Skipping insert row event: {}", event);
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
            Long ts = context.clock().currentTimeInMillis();
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
                    } else {
                        logger.debug("Recorded {} update record(s) for event: {}", count, event);
                    }
                }
            } else {
                // All rows were previously processed ...
                logger.debug("Skipping previously processed update event: {}", event);
            }
        } else {
            logger.debug("Skipping update row event: {}", event);
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
            Long ts = context.clock().currentTimeInMillis();
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
                    } else {
                        logger.debug("Recorded {} delete record(s) for event: {}", count, event);
                    }
                }
            } else {
                // All rows were previously processed ...
                logger.debug("Skipping previously processed delete event: {}", event);
            }
        } else {
            logger.debug("Skipping delete row event: {}", event);
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

    protected SSLMode sslModeFor(SecureConnectionMode mode) {
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
            context.temporaryLoggingContext("binlog", () -> {
                Map<String, ?> offset = lastOffset;
                if (offset != null) {
                    logger.info("Stopped reading binlog after {} events, last recorded offset: {}", totalRecordCounter, offset);
                } else {
                    logger.info("Stopped reading binlog after {} events, no new offset was recorded", totalRecordCounter);
                }
            });
        }

        @Override
        public void onConnect(BinaryLogClient client) {
            // Set up the MDC logging context for this thread ...
            context.configureLoggingContext("binlog");

            // The event row number will be used when processing the first event ...
            logger.info("Connected to MySQL binlog at {}:{}, starting at {}", context.hostname(), context.port(), source);
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
            logger.debug("A deserialization failure event arrived", ex);
            logReaderState();
            BinlogReader.this.failed(ex);
        }
    }

    private void logReaderState() {
        logger.error("Error during binlog processing. Last offset stored = {}, binlog reader near position = {}",
                lastOffset,
                client == null ? "N/A" : client.getBinlogFilename() + "/" + client.getBinlogPosition()
        );
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
}

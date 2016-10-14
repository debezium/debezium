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
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.event.deserialization.GtidEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;
import com.github.shyiko.mysql.binlog.network.SSLMode;

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
    private BinaryLogClient client;
    private int startingRowNumber = 0;
    private final Clock clock;
    private final ElapsedTimeStrategy pollOutputDelay;
    private long recordCounter = 0L;
    private long previousOutputMillis = 0L;
    private final AtomicLong totalRecordCounter = new AtomicLong();
    private volatile Map<String, ?> lastOffset = null;

    /**
     * Create a binlog reader.
     * 
     * @param context the task context in which this reader is running; may not be null
     */
    public BinlogReader(MySqlTaskContext context) {
        super(context);
        source = context.source();
        recordMakers = context.makeRecord();
        recordSchemaChangesInSourceRecords = context.includeSchemaChangeRecords();
        clock = context.clock();

        // Use exponential delay to log the progress frequently at first, but the quickly tapering off to once an hour...
        pollOutputDelay = ElapsedTimeStrategy.exponential(clock, INITIAL_POLL_PERIOD_IN_MILLIS, MAX_POLL_PERIOD_IN_MILLIS);

        // Set up the log reader ...
        client = new BinaryLogClient(context.hostname(), context.port(), context.username(), context.password());
        client.setServerId(context.serverId());
        client.setSSLMode(sslModeFor(context.sslMode()));
        client.setKeepAlive(context.config().getBoolean(MySqlConnectorConfig.KEEP_ALIVE));
        client.registerEventListener(this::handleEvent);
        client.registerLifecycleListener(new ReaderThreadLifecycleListener());
        if (logger.isDebugEnabled()) client.registerEventListener(this::logEvent);

        // Set up the event deserializer with additional type(s) ...
        final Map<Long, TableMapEventData> tableMapEventByTableId = new HashMap<Long, TableMapEventData>();
        EventDeserializer eventDeserializer = new EventDeserializer() {
            @Override
            public Event nextEvent(ByteArrayInputStream inputStream) throws IOException {
                // Delegate to the superclass ...
                Event event = super.nextEvent(inputStream);
                // We have to record the most recent TableMapEventData for each table number for our custom deserializers ...
                if (event.getHeader().getEventType() == EventType.TABLE_MAP) {
                    TableMapEventData tableMapEvent = event.getData();
                    tableMapEventByTableId.put(tableMapEvent.getTableId(), tableMapEvent);
                }
                return event;
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
        eventHandlers.put(EventType.GTID, this::handleGtidEvent);
        eventHandlers.put(EventType.WRITE_ROWS, this::handleInsert);
        eventHandlers.put(EventType.UPDATE_ROWS, this::handleUpdate);
        eventHandlers.put(EventType.DELETE_ROWS, this::handleDelete);
        eventHandlers.put(EventType.EXT_WRITE_ROWS, this::handleInsert);
        eventHandlers.put(EventType.EXT_UPDATE_ROWS, this::handleUpdate);
        eventHandlers.put(EventType.EXT_DELETE_ROWS, this::handleDelete);

        // The 'source' object holds the starting point in the binlog where we should start reading,
        // set set the client to start from that point ...
        String gtidSetStr = source.gtidSet();
        if (gtidSetStr != null) {
            logger.info("GTID set from previous recorded offset: {}", gtidSetStr);
            // Remove any of the GTID sources that are not required/acceptable ...
            Predicate<String> gtidSourceFilter = context.gtidSourceFilter();
            if ( gtidSourceFilter != null) {
                GtidSet gtidSet = new GtidSet(gtidSetStr).retainAll(gtidSourceFilter);
                gtidSetStr = gtidSet.toString();
                logger.info("GTID set after applying GTID source includes/excludes: {}", gtidSetStr);
                source.setGtidSet(gtidSetStr);
            }
            client.setGtidSet(gtidSetStr);
        } else {
            client.setBinlogFilename(source.binlogFilename());
            client.setBinlogPosition(source.nextBinlogPosition());
        }

        // Set the starting row number, which is the next row number to be read ...
        startingRowNumber = source.nextEventRowNumber();

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

    @Override
    protected void doStop() {
        try {
            logger.debug("Stopping binlog reader, last recorded offset: {}", lastOffset);
            client.disconnect();
        } catch (IOException e) {
            logger.error("Unexpected error when disconnecting from the MySQL binary log reader", e);
        }
    }

    @Override
    protected void doCleanup() {
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
        source.setBinlogTimestampSeconds(eventHeader.getTimestamp() / 1000L); // client returns milliseconds, we record seconds
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

            // And after that event has been processed, always set the starting row number to 0 ...
            startingRowNumber = 0;
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
        logger.trace("Server incident: {}", event);
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
        source.setGtid(gtidEvent.getGtid());
        source.setGtidSet(client.getGtidSet());
    }

    /**
     * Handle the supplied event with an {@link QueryEventData} by possibly recording the DDL statements as changes in the
     * MySQL schemas.
     * 
     * @param event the database change data event to be processed; may not be null
     */
    protected void handleQueryEvent(Event event) {
        QueryEventData command = unwrapData(event);
        logger.debug("Received update table command: {}", event);
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
        WriteRowsEventData write = unwrapData(event);
        long tableNumber = write.getTableId();
        BitSet includedColumns = write.getIncludedColumns();
        RecordsForTable recordMaker = recordMakers.forTable(tableNumber, includedColumns, super::enqueueRecord);
        if (recordMaker != null) {
            List<Serializable[]> rows = write.getRows();
            Long ts = context.clock().currentTimeInMillis();
            int count = 0;
            int numRows = rows.size();
            for (int row = startingRowNumber; row != numRows; ++row) {
                count += recordMaker.create(rows.get(row), ts, row, numRows);
            }
            logger.debug("Recorded {} insert records for event: {}", count, event);
        } else {
            logger.debug("Skipping insert row event: {}", event);
        }
    }

    /**
     * Generate source records for the supplied event with an {@link UpdateRowsEventData}.
     * 
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleUpdate(Event event) throws InterruptedException {
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
            for (int row = startingRowNumber; row != numRows; ++row) {
                Map.Entry<Serializable[], Serializable[]> changes = rows.get(row);
                Serializable[] before = changes.getKey();
                Serializable[] after = changes.getValue();
                count += recordMaker.update(before, after, ts, row, numRows);
            }
            logger.debug("Recorded {} update records for event: {}", count, event);
        } else {
            logger.debug("Skipping update row event: {}", event);
        }
    }

    /**
     * Generate source records for the supplied event with an {@link DeleteRowsEventData}.
     * 
     * @param event the database change data event to be processed; may not be null
     * @throws InterruptedException if this thread is interrupted while blocking
     */
    protected void handleDelete(Event event) throws InterruptedException {
        DeleteRowsEventData deleted = unwrapData(event);
        long tableNumber = deleted.getTableId();
        BitSet includedColumns = deleted.getIncludedColumns();
        RecordsForTable recordMaker = recordMakers.forTable(tableNumber, includedColumns, super::enqueueRecord);
        if (recordMaker != null) {
            List<Serializable[]> rows = deleted.getRows();
            Long ts = context.clock().currentTimeInMillis();
            int count = 0;
            int numRows = rows.size();
            for (int row = startingRowNumber; row != numRows; ++row) {
                count += recordMaker.delete(rows.get(row), ts, row, numRows);
            }
            logger.debug("Recorded {} delete records for event: {}", count, event);
        } else {
            logger.debug("Skipping delete row event: {}", event);
        }
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
            BinlogReader.this.failed(ex);
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
            BinlogReader.this.failed(ex);
        }
    }
}

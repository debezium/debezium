/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.AuthenticationException;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 * 
 * @see MySqlConnector
 * @author Randall Hauch
 */
@NotThreadSafe
public final class MySqlConnectorTask extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final TopicSelector topicSelector;
    private final Clock clock = Clock.system();
    private final AtomicBoolean running = new AtomicBoolean(false);

    // These are all effectively constants between start(...) and stop(...)
    private MySqlSchema dbSchema;
    private String serverName;
    private int maxBatchSize;
    private BlockingQueue<Event> events;
    private Queue<Event> batchEvents;
    private EnumMap<EventType, EventHandler> eventHandlers = new EnumMap<>(EventType.class);
    private TableConverters tableConverters;
    private Metronome metronome;
    private BinaryLogClient client;

    // Used in the methods that process events ...
    private final SourceInfo source = new SourceInfo();

    /**
     * Create an instance of the log reader that uses Kafka to store database schema history and the
     * {@link TopicSelector#defaultSelector() default topic selector} of "{@code <serverName>.<databaseName>.<tableName>}" for
     * data and "{@code <serverName>}" for metadata.
     */
    public MySqlConnectorTask() {
        this.topicSelector = TopicSelector.defaultSelector();
        this.dbSchema = null; // delay creating the history until startup, which is only allowed by default constructor
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        if (context == null) {
            throw new ConnectException("Unexpected null context");
        }

        // Validate the configuration ...
        final Configuration config = Configuration.from(props);
        if (!config.validate(MySqlConnectorConfig.ALL_FIELDS, logger::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        // Create and configure the database history ...
        this.dbSchema = new MySqlSchema(config);
        this.dbSchema.start();
        this.running.set(true);

        // Read the configuration ...
        final String user = config.getString(MySqlConnectorConfig.USER);
        final String password = config.getString(MySqlConnectorConfig.PASSWORD);
        final String host = config.getString(MySqlConnectorConfig.HOSTNAME);
        final int port = config.getInteger(MySqlConnectorConfig.PORT);
        final String initialBinLogFilename = config.getString(MySqlConnectorConfig.INITIAL_BINLOG_FILENAME);
        final long serverId = config.getLong(MySqlConnectorConfig.SERVER_ID);
        serverName = config.getString(MySqlConnectorConfig.SERVER_NAME.name(), host + ":" + port);
        final boolean keepAlive = config.getBoolean(MySqlConnectorConfig.KEEP_ALIVE);
        final int maxQueueSize = config.getInteger(MySqlConnectorConfig.MAX_QUEUE_SIZE);
        final long timeoutInMilliseconds = config.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS);
        final boolean includeSchemaChanges = config.getBoolean(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES);
        final long pollIntervalMs = config.getLong(MySqlConnectorConfig.POLL_INTERVAL_MS);
        maxBatchSize = config.getInteger(MySqlConnectorConfig.MAX_BATCH_SIZE);
        metronome = Metronome.parker(pollIntervalMs, TimeUnit.MILLISECONDS, Clock.SYSTEM);

        // Create the queue ...
        events = new LinkedBlockingDeque<>(maxQueueSize);
        batchEvents = new ArrayDeque<>(maxBatchSize);

        // Set up our handlers for specific kinds of events ...
        tableConverters = new TableConverters(topicSelector, dbSchema, clock, includeSchemaChanges);
        eventHandlers.put(EventType.ROTATE, tableConverters::rotateLogs);
        eventHandlers.put(EventType.TABLE_MAP, tableConverters::updateTableMetadata);
        eventHandlers.put(EventType.QUERY, tableConverters::updateTableCommand);
        eventHandlers.put(EventType.EXT_WRITE_ROWS, tableConverters::handleInsert);
        eventHandlers.put(EventType.EXT_UPDATE_ROWS, tableConverters::handleUpdate);
        eventHandlers.put(EventType.EXT_DELETE_ROWS, tableConverters::handleDelete);

        // Set up the log reader ...
        client = new BinaryLogClient(host, port, user, password);
        client.setServerId(serverId);
        client.setKeepAlive(keepAlive);
        if (logger.isDebugEnabled()) client.registerEventListener(this::logEvent);
        client.registerEventListener(this::enqueue);
        client.registerLifecycleListener(traceLifecycleListener());

        // Set up the event deserializer with additional types ...
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setEventDataDeserializer(EventType.STOP, new StopEventDataDeserializer());
        client.setEventDeserializer(eventDeserializer);

        // Check if we've already processed some of the log for this database ...
        source.setServerName(serverName);
        
        // We use the initial binlog filename configuration property to know whether to perform a snapshot
        // or to start with that (or the previous) binlog position...
        if ( initialBinLogFilename == null || initialBinLogFilename.trim().isEmpty() ) {
            // No initial binlog filename was specified, so perform a snapshot ...
        }
        
        // Get the offsets for our partition ...
        Map<String, ?> offsets = context.offsetStorageReader().offset(source.partition());
        if (offsets != null) {
            source.setOffset(offsets);
            // And set the client to start from that point ...
            client.setBinlogFilename(source.binlogFilename());
            client.setBinlogPosition(source.binlogPosition());
            // The event row number will be used when processing the first event ...
            logger.info("Restarting MySQL connector '{}' from binlog file {}, position {}, and event row {}",
                        serverName, source.binlogFilename(), source.binlogPosition(), source.eventRowNumber());

            // We have to make our Tables reflect the state of the database at the above source partition (e.g., the location
            // in the MySQL log where we last stopped reading. Since the TableConverts writes out all DDL statements to the
            // TopicSelector.getTopic(serverName) topic, we can consume that topic and apply each of the DDL statements
            // to our Tables object. Each of those DDL messages is keyed by the database name, and contains a single string
            // of DDL. However, we should consume no further than offset we recovered above.
            try {
                logger.info("Recovering MySQL connector '{}' database schemas from history stored in {}", serverName, dbSchema.historyLocation());
                dbSchema.loadHistory(source);
                logger.debug("Recovered MySQL connector '{}' database schemas: {}", serverName, dbSchema.tables());
            } catch (Throwable t) {
                throw new ConnectException("Failure while recovering database schemas", t);
            }
        } else {
            // initializes this position, though it will be reset when we see the first event (should be a rotate event) ...
            client.setBinlogFilename(initialBinLogFilename);
            logger.info("Starting MySQL connector '{}' from beginning of binlog file {}, position {}",
                        serverName, source.binlogFilename(), source.binlogPosition());
        }

        // Start the log reader, which starts background threads ...
        try {
            logger.debug("Connecting to MySQL server");
            client.connect(timeoutInMilliseconds);
            logger.info("Successfully started MySQL Connector '{}' and beginning to read binlog", serverName);
        } catch (TimeoutException e) {
            double seconds = TimeUnit.MILLISECONDS.toSeconds(timeoutInMilliseconds);
            throw new ConnectException("Timed out after " + seconds + " seconds while waiting to connect to the MySQL database at " + host
                    + ":" + port + " with user '" + user + "'", e);
        } catch (AuthenticationException e) {
            throw new ConnectException("Failed to authenticate to the MySQL database at " + host + ":" + port + " with user '" + user + "'",
                    e);
        } catch (Throwable e) {
            throw new ConnectException(
                    "Unable to connect to the MySQL database at " + host + ":" + port + " with user '" + user + "': " + e.getMessage(), e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        logger.trace("Polling for events from MySQL server '{}'", serverName);
        while (running.get() && (events.drainTo(batchEvents, maxBatchSize - batchEvents.size()) == 0 || batchEvents.isEmpty())) {
            // No events to process, so sleep for a bit ...
            metronome.pause();
        }
        logger.trace("Preparing {} events from MySQL server '{}'", events.size(), serverName);

        // We have at least some records to process ...
        List<SourceRecord> records = new ArrayList<>(batchEvents.size());
        while (!batchEvents.isEmpty()) {
            Event event = batchEvents.poll();
            if (event == null) continue;

            // Update the source offset info ...
            EventHeader eventHeader = event.getHeader();
            source.setBinlogTimestamp(eventHeader.getTimestamp());
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
                source.setBinlogFilename(rotateEventData.getBinlogFilename());
                source.setBinlogPosition(rotateEventData.getBinlogPosition());
                source.setRowInEvent(0);
            } else if (eventHeader instanceof EventHeaderV4) {
                EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
                long nextBinlogPosition = trackableEventHeader.getNextPosition();
                if (nextBinlogPosition > 0) {
                    source.setBinlogPosition(nextBinlogPosition);
                    source.setRowInEvent(0);
                }
            }

            if (!running.get()) break;

            // If there is a handler for this event, forward the event to it ...
            EventHandler handler = eventHandlers.get(eventType);
            if (handler != null) {
                handler.handle(event, source, records::add);
            }
        }
        logger.trace("Completed processing {} events from MySQL server '{}'", serverName);

        if (!this.running.get()) {
            // We're supposed to stop, so return nothing that we might have already processed
            // so that no records get persisted if DB history has already been stopped ...
            return null;
        }

        // We've processed them all, so clear the batch and return the records ...
        assert batchEvents.isEmpty();
        return records;
    }

    @Override
    public void stop() {
        try {
            logger.info("Stopping MySQL Connector '{}'", serverName);
            // Signal to the 'poll()' method that it should stop what its doing ...
            this.running.set(false);

            // Flush and stop the database history ...
            logger.debug("Stopping database history for MySQL server '{}'", serverName);
            dbSchema.shutdown();
        } catch (Throwable e) {
            logger.error("Unexpected error shutting down the database history", e);
        } finally {
            try {
                logger.debug("Disconnecting from MySQL server '{}'", serverName);
                client.disconnect();
                logger.info("Stopped connector to MySQL server '{}'", serverName);
            } catch (IOException e) {
                logger.error("Unexpected error when disconnecting from the MySQL binary log reader", e);
            }
        }
    }

    /**
     * Adds the event into the queue for subsequent batch processing.
     * 
     * @param event the event that was read from the binary log
     */
    protected void enqueue(Event event) {
        if (event != null) {
            try {
                events.put(event);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ConnectException("Interrupted while waiting to add event to queue", e);
            }
        }
    }

    protected void logEvent(Event event) {
        logger.debug("Received event: " + event);
    }

    protected void sleep(long timeInMillis) {
        try {
            Thread.sleep(timeInMillis);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    protected LifecycleListener traceLifecycleListener() {
        return new LifecycleListener() {
            @Override
            public void onDisconnect(BinaryLogClient client) {
                logger.debug("MySQL Connector disconnected");
            }

            @Override
            public void onConnect(BinaryLogClient client) {
                logger.info("MySQL Connector connected");
            }

            @Override
            public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
                logger.error("MySQL Connector communication failure", ex);
            }

            @Override
            public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
                logger.error("MySQL Connector received event deserialization failure", ex);
            }
        };
    }

    /**
     * The functional interface for all event handler methods.
     */
    @FunctionalInterface
    protected static interface EventHandler {
        void handle(Event event, SourceInfo source, Consumer<SourceRecord> recorder);
    }
}

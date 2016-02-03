/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;

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
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistory;

/**
 * A Kafka Connect source task reads the MySQL binary log and generate the corresponding data change events.
 * 
 * @see MySqlConnector
 * @author Randall Hauch
 */
@NotThreadSafe
final class LogReader extends SourceTask {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final TopicSelector topicSelector;

    // These are all effectively constants between start(...) and stop(...)
    private DatabaseHistory dbHistory;
    private EnumMap<EventType, EventHandler> eventHandlers = new EnumMap<>(EventType.class);
    private Tables tables;
    private TableConverters tableConverters;
    private BinaryLogClient client;
    private BlockingQueue<Event> events;
    private List<Event> batchEvents;
    private int maxBatchSize;
    private long pollIntervalMs;

    // Used in the methods that process events ...
    private final SourceInfo source = new SourceInfo();

    /**
     * Create an instance of the log reader that uses Kafka to store database schema history and the
     * {@link TopicSelector#defaultSelector() default topic selector} of "{@code <serverName>.<databaseName>.<tableName>}" for
     * data and "{@code <serverName>}" for metadata.
     */
    public LogReader() {
        this.topicSelector = TopicSelector.defaultSelector();
        this.dbHistory = null; // delay creating the history until startup, which is only allowed by default constructor
    }

    /**
     * Create an instance of the log reader that uses the supplied {@link TopicSelector} and the supplied storage for database
     * schema history.
     * 
     * @param dbHistory the history storage for the database's schema; may not be null
     * @param dataTopicSelector the selector for topics where data and metadata changes are to be written; if null the
     *            {@link TopicSelector#defaultSelector() default topic selector} will be used
     */
    protected LogReader(DatabaseHistory dbHistory, TopicSelector dataTopicSelector) {
        Objects.requireNonNull(dbHistory, "The storage for database schema history is required");
        this.topicSelector = dataTopicSelector != null ? dataTopicSelector : TopicSelector.defaultSelector();
        this.dbHistory = dbHistory;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        // Validate the configuration ...
        final Configuration config = Configuration.from(props);
        if (config.validate(MySqlConnectorConfig.ALL_FIELDS, logger::error)) {
            return;
        }

        // Create and configure the database history ...
        this.dbHistory = config.getInstance(MySqlConnectorConfig.DATABASE_HISTORY, DatabaseHistory.class);
        if (this.dbHistory == null) {
            this.logger.error("Unable to instantiate the database history class {}",
                              config.getString(MySqlConnectorConfig.DATABASE_HISTORY));
            return;
        }
        Configuration dbHistoryConfig = config.subset(DatabaseHistory.CONFIG_PREFIX, false); // do not remove prefix
        this.dbHistory.configure(dbHistoryConfig);
        this.dbHistory.start();

        // Read the configuration ...
        final String user = config.getString(MySqlConnectorConfig.USER);
        final String password = config.getString(MySqlConnectorConfig.PASSWORD);
        final String host = config.getString(MySqlConnectorConfig.HOSTNAME);
        final int port = config.getInteger(MySqlConnectorConfig.PORT);
        final Long serverId = config.getLong(MySqlConnectorConfig.SERVER_ID);
        final String serverName = config.getString(MySqlConnectorConfig.SERVER_NAME.name(), host + ":" + port);
        final boolean keepAlive = config.getBoolean(MySqlConnectorConfig.KEEP_ALIVE);
        final int maxQueueSize = config.getInteger(MySqlConnectorConfig.MAX_QUEUE_SIZE);
        final long timeoutInMilliseconds = config.getLong(MySqlConnectorConfig.CONNECTION_TIMEOUT_MS);
        final boolean includeSchemaChanges = config.getBoolean(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES);
        maxBatchSize = config.getInteger(MySqlConnectorConfig.MAX_BATCH_SIZE);
        pollIntervalMs = config.getLong(MySqlConnectorConfig.POLL_INTERVAL_MS);
        if (maxQueueSize <= maxBatchSize) {
            maxBatchSize = maxQueueSize / 2;
            logger.error("The {} value must be larger than {}, so changing {} to {}", MySqlConnectorConfig.MAX_QUEUE_SIZE,
                         MySqlConnectorConfig.MAX_BATCH_SIZE, MySqlConnectorConfig.MAX_QUEUE_SIZE, maxBatchSize);
        }

        // Define the filter using the whitelists and blacklists for tables and database names ...
        Predicate<TableId> tableFilter = TableId.filter(config.getString(MySqlConnectorConfig.DATABASE_WHITELIST),
                                                        config.getString(MySqlConnectorConfig.DATABASE_BLACKLIST),
                                                        config.getString(MySqlConnectorConfig.TABLE_WHITELIST),
                                                        config.getString(MySqlConnectorConfig.TABLE_BLACKLIST));

        // Create the queue ...
        events = new LinkedBlockingDeque<>(maxQueueSize);
        batchEvents = new ArrayList<>(maxBatchSize);

        // Set up our handlers for specific kinds of events ...
        tables = new Tables();
        tableConverters = new TableConverters(topicSelector, dbHistory, includeSchemaChanges, tables, tableFilter);
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

        // Check if we've already processed some of the log for this database ...
        source.setServerName(serverName);
        if (context != null) {
            // Get the offsets for our partition ...
            source.setOffset(context.offsetStorageReader().offset(source.partition()));
            // And set the client to start from that point ...
            client.setBinlogFilename(source.binlogFilename());
            client.setBinlogPosition(source.binlogPosition());
            // The event row number will be used when processing the first event ...

            // We have to make our Tables reflect the state of the database at the above source partition (e.g., the location
            // in the MySQL log where we last stopped reading. Since the TableConverts writes out all DDL statements to the
            // TopicSelector.getTopic(serverName) topic, we can consume that topic and apply each of the DDL statements
            // to our Tables object. Each of those DDL messages is keyed by the database name, and contains a single string
            // of DDL. However, we should consume no further than offset we recovered above.
            try {
                DdlParser ddlParser = new MySqlDdlParser();
                dbHistory.recover(source.partition(), source.offset(), tables, ddlParser);
            } catch (Throwable t) {
                logger.error("Error while recovering database schemas", t);
            }
        } else {
            // initializes this position, though it will be reset when we see the first event (should be a rotate event) ...
            source.setBinlogPosition(client.getBinlogPosition());
        }

        // Start the log reader, which starts background threads ...
        try {
            client.connect(timeoutInMilliseconds);
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
        while (events.drainTo(batchEvents, maxBatchSize - batchEvents.size()) == 0 || batchEvents.isEmpty()) {
            // No events to process, so sleep for a bit ...
            sleep(pollIntervalMs);
        }

        // We have at least some records to process ...
        List<SourceRecord> records = new ArrayList<>(batchEvents.size());
        for (Event event : batchEvents) {

            // Update the source offset info ...
            EventHeader eventHeader = event.getHeader();
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

            // If there is a handler for this event, forward the event to it ...
            EventHandler handler = eventHandlers.get(eventType);
            if (handler != null) {
                handler.handle(event, source, records::add);
            }
        }
        // We've processed them all, so clear the batch and return the records ...
        batchEvents.clear();
        return records;
    }

    @Override
    public void stop() {
        try {
            dbHistory.stop();
        } catch (Throwable e) {
            logger.error("Unexpected error shutting down the database history", e);
        } finally {
            try {
                client.disconnect();
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
        if (event != null) events.add(event);
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

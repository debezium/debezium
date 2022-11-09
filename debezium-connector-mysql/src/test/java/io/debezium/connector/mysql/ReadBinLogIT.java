/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.github.shyiko.mysql.binlog.network.ServerException;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, patch = 5, reason = "MySQL 5.5 does not support CURRENT_TIMESTAMP on DATETIME and only a single column can specify default CURRENT_TIMESTAMP, lifted in MySQL 5.6.5")
public class ReadBinLogIT implements Testing {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ReadBinLogIT.class);
    protected static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(15);

    private static final class AnyValue implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static final Serializable ANY_OBJECT = new AnyValue();

    private EventQueue counters;
    private BinaryLogClient client;
    private MySqlTestConnection conn;
    private List<Event> events = new LinkedList<>();
    private JdbcConfiguration config;

    private final UniqueDatabase DATABASE = new UniqueDatabase("readbinlog_it", "readbinlog_test");

    @Rule
    public SkipTestRule skipTest = new SkipTestRule();

    @Before
    public void beforeEach() throws TimeoutException, IOException, SQLException, InterruptedException {
        events.clear();

        // Connect the normal SQL client ...
        DATABASE.createAndInitialize();
        conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
        conn.connect();

        // Get the configuration that we used ...
        config = conn.config();
    }

    @After
    public void afterEach() throws IOException, SQLException {
        events.clear();
        try {
            if (client != null) {
                client.disconnect();
            }
        }
        finally {
            client = null;
            try {
                if (conn != null) {
                    conn.close();
                }
            }
            finally {
                conn = null;
            }
        }
    }

    protected void startClient() throws IOException, TimeoutException, SQLException {
        startClient(null);
    }

    protected void startClient(Consumer<BinaryLogClient> preConnect) throws IOException, TimeoutException, SQLException {
        // Connect the bin log client ...
        counters = new EventQueue(DEFAULT_TIMEOUT, this::logConsumedEvent, this::logIgnoredEvent);
        client = new BinaryLogClient(config.getHostname(), config.getPort(), "replicator", "replpass");
        client.setServerId(client.getServerId() - 1); // avoid clashes between BinaryLogClient instances
        client.setKeepAlive(false);
        client.setSSLMode(SSLMode.DISABLED);
        client.registerEventListener(counters);
        client.registerEventListener(this::recordEvent);
        client.registerLifecycleListener(new TraceLifecycleListener());
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setEventDataDeserializer(EventType.STOP, new StopEventDataDeserializer());
        client.setEventDeserializer(eventDeserializer);
        if (preConnect != null) {
            preConnect.accept(client);
        }
        client.connect(DEFAULT_TIMEOUT); // does not block

        // Set up the table as one transaction and wait to see the events ...
        conn.execute("DROP TABLE IF EXISTS person",
                "CREATE TABLE person (" +
                        "  name VARCHAR(255) primary key," +
                        "  age INTEGER NULL DEFAULT 10," +
                        "  createdAt DATETIME NULL DEFAULT CURRENT_TIMESTAMP," +
                        "  updatedAt DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                        ")");

        counters.consume(2, EventType.QUERY);
        counters.reset();
    }

    @Ignore
    @Test(expected = ServerException.class)
    public void shouldFailToConnectToInvalidBinlogFile() throws Exception {
        Testing.Print.enable();
        startClient(client -> {
            client.setBinlogFilename("invalid-mysql-binlog.filename.000001");
        });
    }

    @Ignore
    @Test
    public void shouldReadMultipleBinlogFiles() throws Exception {
        Testing.Print.enable();
        startClient(client -> {
            client.setBinlogFilename("mysql-bin.000001");
        });
        counters.consumeAll(20, TimeUnit.SECONDS);
    }

    @Test
    public void shouldCaptureSingleWriteUpdateDeleteEvents() throws Exception {
        String sslMode = System.getProperty("database.ssl.mode", "disabled");

        // not running this test with SSL, there's enough coverage of that elsewhere and setting up
        // the plain client with the right config seems not worth the effort
        if (!sslMode.equals("disabled")) {
            return;
        }

        startClient();
        // Testing.Print.enable();
        // write/insert
        conn.execute("INSERT INTO person(name,age) VALUES ('Georgia',30)");
        counters.consume(1, WriteRowsEventData.class);
        List<WriteRowsEventData> writeRowEvents = recordedEventData(WriteRowsEventData.class, 1);
        assertRows(writeRowEvents.get(0), rows().insertedRow("Georgia", 30, any(), any()));

        // update
        conn.execute("UPDATE person SET name = 'Maggie' WHERE name = 'Georgia'");
        counters.consume(1, UpdateRowsEventData.class);
        List<UpdateRowsEventData> updateRowEvents = recordedEventData(UpdateRowsEventData.class, 1);
        assertRows(updateRowEvents.get(0),
                rows().changeRow("Georgia", 30, any(), any()).to("Maggie", 30, any(), any()));

        // delete
        conn.execute("DELETE FROM person WHERE name = 'Maggie'");
        counters.consume(1, DeleteRowsEventData.class);
        List<DeleteRowsEventData> deleteRowEvents = recordedEventData(DeleteRowsEventData.class, 1);
        assertRows(deleteRowEvents.get(0), rows().removedRow("Maggie", 30, any(), any()));
    }

    @Test
    public void shouldCaptureMultipleWriteUpdateDeleteEvents() throws Exception {
        String sslMode = System.getProperty("database.ssl.mode", "disabled");

        // not running this test with SSL, there's enough coverage of that elsewhere and setting up
        // the plain client with the right config seems not worth the effort
        if (!sslMode.equals("disabled")) {
            return;
        }

        startClient();
        // write/insert as a single transaction
        conn.execute("INSERT INTO person(name,age) VALUES ('Georgia',30)",
                "INSERT INTO person(name,age) VALUES ('Janice',19)");
        counters.consume(1, QueryEventData.class); // BEGIN
        counters.consume(1, TableMapEventData.class);
        counters.consume(2, WriteRowsEventData.class);
        counters.consume(1, XidEventData.class); // COMMIT
        List<WriteRowsEventData> writeRowEvents = recordedEventData(WriteRowsEventData.class, 2);
        assertRows(writeRowEvents.get(0), rows().insertedRow("Georgia", 30, any(), any()));
        assertRows(writeRowEvents.get(1), rows().insertedRow("Janice", 19, any(), any()));
        counters.reset();

        // update as a single transaction
        conn.execute("UPDATE person SET name = 'Maggie' WHERE name = 'Georgia'",
                "UPDATE person SET name = 'Jamie' WHERE name = 'Janice'");
        counters.consume(1, QueryEventData.class); // BEGIN
        counters.consume(1, TableMapEventData.class);
        counters.consume(2, UpdateRowsEventData.class);
        counters.consume(1, XidEventData.class); // COMMIT
        List<UpdateRowsEventData> updateRowEvents = recordedEventData(UpdateRowsEventData.class, 2);
        assertRows(updateRowEvents.get(0), rows().changeRow("Georgia", 30, any(), any()).to("Maggie", 30, any(), any()));
        assertRows(updateRowEvents.get(1), rows().changeRow("Janice", 19, any(), any()).to("Jamie", 19, any(), any()));
        counters.reset();

        // delete as a single transaction
        conn.execute("DELETE FROM person WHERE name = 'Maggie'",
                "DELETE FROM person WHERE name = 'Jamie'");
        counters.consume(1, QueryEventData.class); // BEGIN
        counters.consume(1, TableMapEventData.class);
        counters.consume(2, DeleteRowsEventData.class);
        counters.consume(1, XidEventData.class); // COMMIT
        List<DeleteRowsEventData> deleteRowEvents = recordedEventData(DeleteRowsEventData.class, 2);
        assertRows(deleteRowEvents.get(0), rows().removedRow("Maggie", 30, any(), any()));
        assertRows(deleteRowEvents.get(1), rows().removedRow("Jamie", 19, any(), any()));
    }

    @Test
    public void shouldCaptureMultipleWriteUpdateDeletesInSingleEvents() throws Exception {
        String sslMode = System.getProperty("database.ssl.mode", "disabled");

        // not running this test with SSL, there's enough coverage of that elsewhere and setting up
        // the plain client with the right config seems not worth the effort
        if (!sslMode.equals("disabled")) {
            return;
        }

        startClient();
        // write/insert as a single statement/transaction
        conn.execute("INSERT INTO person(name,age) VALUES ('Georgia',30),('Janice',19)");
        counters.consume(1, QueryEventData.class); // BEGIN
        counters.consume(1, TableMapEventData.class);
        counters.consume(1, WriteRowsEventData.class);
        counters.consume(1, XidEventData.class); // COMMIT
        List<WriteRowsEventData> writeRowEvents = recordedEventData(WriteRowsEventData.class, 1);
        assertRows(writeRowEvents.get(0), rows().insertedRow("Georgia", 30, any(), any())
                .insertedRow("Janice", 19, any(), any()));
        counters.reset();

        // update as a single statement/transaction
        conn.execute("UPDATE person SET name = CASE " +
                "                          WHEN name = 'Georgia' THEN 'Maggie' " +
                "                          WHEN name = 'Janice' THEN 'Jamie' " +
                "                         END " +
                "WHERE name IN ('Georgia','Janice')");
        counters.consume(1, QueryEventData.class); // BEGIN
        counters.consume(1, TableMapEventData.class);
        counters.consume(1, UpdateRowsEventData.class);
        counters.consume(1, XidEventData.class); // COMMIT
        List<UpdateRowsEventData> updateRowEvents = recordedEventData(UpdateRowsEventData.class, 1);
        assertRows(updateRowEvents.get(0), rows().changeRow("Georgia", 30, any(), any()).to("Maggie", 30, any(), any())
                .changeRow("Janice", 19, any(), any()).to("Jamie", 19, any(), any()));
        counters.reset();

        // delete as a single statement/transaction
        conn.execute("DELETE FROM person WHERE name IN ('Maggie','Jamie')");
        counters.consume(1, QueryEventData.class); // BEGIN
        counters.consume(1, TableMapEventData.class);
        counters.consume(1, DeleteRowsEventData.class);
        counters.consume(1, XidEventData.class); // COMMIT
        List<DeleteRowsEventData> deleteRowEvents = recordedEventData(DeleteRowsEventData.class, 1);
        assertRows(deleteRowEvents.get(0), rows().removedRow("Maggie", 30, any(), any())
                .removedRow("Jamie", 19, any(), any()));
    }

    /**
     * Test case that is normally commented out since it is only useful to print out the DDL statements recorded by
     * the binlog during a MySQL server initialization and startup.
     *
     * @throws Exception if there are problems
     */
    @Ignore
    @Test
    public void shouldCaptureQueryEventData() throws Exception {
        // Testing.Print.enable();
        startClient(client -> {
            client.setBinlogFilename("mysql-bin.000001");
            client.setBinlogPosition(4);
        });
        counters.consumeAll(5, TimeUnit.SECONDS);
        List<QueryEventData> allQueryEvents = recordedEventData(QueryEventData.class, -1);
        allQueryEvents.forEach(event -> {
            String sql = event.getSql();
            if (sql.equalsIgnoreCase("BEGIN") || sql.equalsIgnoreCase("COMMIT")) {
                return;
            }
            System.out.println(event.getSql());
        });
    }

    @Test
    public void shouldQueryInformationSchema() throws Exception {
        // long tableId = writeRows.getTableId();
        // BitSet columnIds = writeRows.getIncludedColumns();
        //
        // conn.query("select TABLE_NAME, ROW_FORMAT, TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH,
        // DATA_FREE, " +
        // "AUTO_INCREMENT, CREATE_TIME, UPDATE_TIME, CHECK_TIME, TABLE_COLLATION, CHECKSUM, CREATE_OPTIONS, TABLE_COMMENT " +
        // "from INFORMATION_SCHEMA.TABLES " +
        // "where TABLE_SCHEMA like 'readbinlog_test' and TABLE_NAME like 'person'", conn::print);
        // conn.query("select TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, " +
        // "DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, " +
        // "CHARACTER_SET_NAME, COLLATION_NAME from INFORMATION_SCHEMA.COLUMNS " +
        // "where TABLE_SCHEMA like 'readbinlog_test' and TABLE_NAME like 'person'", conn::print);

    }

    protected void logConsumedEvent(Event event) {
        Testing.print("Consumed event: " + event);
    }

    protected void logIgnoredEvent(Event event) {
        Testing.print("Ignored event:  " + event);
    }

    protected void recordEvent(Event event) {
        synchronized (events) {
            events.add(event);
        }
    }

    protected <T extends EventData> List<T> recordedEventData(Class<T> eventType, int expectedCount) {
        List<T> results = null;
        synchronized (events) {
            results = events.stream().map(Event::getData).filter(eventType::isInstance).map(eventType::cast).collect(Collectors.toList());
        }
        if (expectedCount > -1) {
            assertThat(results.size()).isEqualTo(expectedCount);
        }
        return results;
    }

    protected void assertRow(Serializable[] data, Serializable... expected) {
        assertThat(data.length).isEqualTo(expected.length);
        assertThat(data).contains((Object[]) expected);
    }

    protected void assertRows(WriteRowsEventData eventData, int numRowsInEvent, Serializable... expectedValuesInRows) {
        assertThat(eventData.getRows().size()).isEqualTo(numRowsInEvent);
        int valuePosition = 0;
        for (Serializable[] row : eventData.getRows()) {
            for (Serializable value : row) {
                assertThat(value).isEqualTo(expectedValuesInRows[valuePosition++]);
            }
        }
    }

    protected Serializable any() {
        return ANY_OBJECT;
    }

    public static class Row {
        public Serializable[] fromValues;
        public Serializable[] toValues;

    }

    public interface UpdateBuilder {
        RowBuilder to(Serializable... values);
    }

    public static class RowBuilder {
        private List<Row> rows = new ArrayList<>();
        private Row nextRow = null;

        public RowBuilder insertedRow(Serializable... values) {
            maybeAddRow();
            return changeRow().to(values);
        }

        public RowBuilder removedRow(Serializable... values) {
            maybeAddRow();
            return changeRow(values).to(values);
        }

        public UpdateBuilder changeRow(Serializable... values) {
            maybeAddRow();
            nextRow = new Row();
            nextRow.fromValues = values;
            return new UpdateBuilder() {
                @Override
                public RowBuilder to(Serializable... values) {
                    nextRow.toValues = values;
                    return RowBuilder.this;
                }
            };
        }

        protected void maybeAddRow() {
            if (nextRow != null) {
                rows.add(nextRow);
                nextRow = null;
            }
        }

        protected List<Row> rows() {
            maybeAddRow();
            return rows;
        }

        protected boolean findInsertedRow(Serializable[] values) {
            maybeAddRow();
            for (Iterator<Row> iter = rows.iterator(); iter.hasNext();) {
                Row expectedRow = iter.next();
                if (deepEquals(expectedRow.toValues, values)) {
                    iter.remove();
                    return true;
                }
            }
            return false;
        }

        protected boolean findDeletedRow(Serializable[] values) {
            maybeAddRow();
            for (Iterator<Row> iter = rows.iterator(); iter.hasNext();) {
                Row expectedRow = iter.next();
                if (deepEquals(expectedRow.fromValues, values)) {
                    iter.remove();
                    return true;
                }
            }
            return false;
        }

        protected boolean findUpdatedRow(Serializable[] oldValues, Serializable[] newValues) {
            maybeAddRow();
            for (Iterator<Row> iter = rows.iterator(); iter.hasNext();) {
                Row expectedRow = iter.next();
                if (deepEquals(expectedRow.fromValues, oldValues) && deepEquals(expectedRow.toValues, newValues)) {
                    iter.remove();
                    return true;
                }
            }
            return false;
        }

        protected boolean deepEquals(Serializable[] expectedValues, Serializable[] actualValues) {
            assertThat(expectedValues.length).isEqualTo(actualValues.length);
            // Make a copy of the actual values, and find all 'AnyValue' instances in the expected values and replace
            // their counterpart in the copy of the actual values ...
            Serializable[] actualValuesCopy = Arrays.copyOf(actualValues, actualValues.length);
            for (int i = 0; i != actualValuesCopy.length; ++i) {
                if (expectedValues[i] instanceof AnyValue) {
                    actualValuesCopy[i] = expectedValues[i];
                }
            }
            // Now compare the arrays ...
            return Arrays.deepEquals(expectedValues, actualValuesCopy);
        }
    }

    protected RowBuilder rows() {
        return new RowBuilder();
    }

    protected void assertRows(UpdateRowsEventData eventData, RowBuilder rows) {
        assertThat(eventData.getRows().size()).isEqualTo(rows.rows().size());
        for (Map.Entry<Serializable[], Serializable[]> row : eventData.getRows()) {
            if (!rows.findUpdatedRow(row.getKey(), row.getValue())) {
                fail("Failed to find updated row: " + eventData);
            }
        }
    }

    protected void assertRows(WriteRowsEventData eventData, RowBuilder rows) {
        assertThat(eventData.getRows().size()).isEqualTo(rows.rows().size());
        for (Serializable[] removedRow : eventData.getRows()) {
            if (!rows.findInsertedRow(removedRow)) {
                fail("Failed to find inserted row: " + eventData);
            }
        }
    }

    protected void assertRows(DeleteRowsEventData eventData, RowBuilder rows) {
        assertThat(eventData.getRows().size()).isEqualTo(rows.rows().size());
        for (Serializable[] removedRow : eventData.getRows()) {
            if (!rows.findDeletedRow(removedRow)) {
                fail("Failed to find removed row: " + eventData);
            }
        }
    }

    protected static class EventQueue implements EventListener {

        private final ConcurrentLinkedQueue<Event> queue = new ConcurrentLinkedQueue<>();
        private final Consumer<Event> consumedEvents;
        private final Consumer<Event> ignoredEvents;
        private final long defaultTimeoutInMillis;

        public EventQueue(long defaultTimeoutInMillis, Consumer<Event> consumedEvents, Consumer<Event> ignoredEvents) {
            this.defaultTimeoutInMillis = defaultTimeoutInMillis;
            this.consumedEvents = consumedEvents != null ? consumedEvents : this::defaultEventHandler;
            this.ignoredEvents = ignoredEvents != null ? ignoredEvents : this::defaultEventHandler;
        }

        private void defaultEventHandler(Event event) {
        }

        @Override
        public void onEvent(Event event) {
            boolean success = queue.offer(event);
            assert success;
        }

        /**
         * Blocks for the specified amount of time, consuming (and discarding) all events.
         *
         * @param timeout the maximum amount of time that this method should block
         * @param unit the time unit for {@code timeout}
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void consumeAll(long timeout, TimeUnit unit) throws TimeoutException {
            final long stopTime = System.currentTimeMillis() + unit.toMillis(timeout);
            while (System.currentTimeMillis() < stopTime) {
                Event nextEvent = queue.poll();
                if (nextEvent != null) {
                    Testing.print("Found event: " + nextEvent);
                    consumedEvents.accept(nextEvent);
                }
            }
        }

        /**
         * Blocks until the listener has consume the specified number of matching events, blocking at most the default number of
         * milliseconds. If this method has not reached the number of matching events and comes across events that do not satisfy
         * the predicate, those events are consumed and ignored.
         *
         * @param eventCount the number of events
         * @param condition the event-based predicate that signals a match; may not be null
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void consume(int eventCount, Predicate<Event> condition) throws TimeoutException {
            consume(eventCount, defaultTimeoutInMillis, condition);
        }

        /**
         * Blocks until the listener has consume the specified number of matching events, blocking at most the specified number
         * of milliseconds. If this method has not reached the number of matching events and comes across events that do not
         * satisfy the predicate, those events are consumed and ignored.
         *
         * @param eventCount the number of events
         * @param timeoutInMillis the maximum amount of time in milliseconds that this method should block
         * @param condition the event-based predicate that signals a match; may not be null
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void consume(int eventCount, long timeoutInMillis, Predicate<Event> condition)
                throws TimeoutException {
            if (eventCount < 0) {
                throw new IllegalArgumentException("The eventCount may not be negative");
            }
            if (eventCount == 0) {
                return;
            }
            int eventsRemaining = eventCount;
            final long stopTime = System.currentTimeMillis() + timeoutInMillis;
            while (eventsRemaining > 0 && System.currentTimeMillis() < stopTime) {
                Event nextEvent = queue.poll();
                if (nextEvent != null) {
                    if (condition.test(nextEvent)) {
                        --eventsRemaining;
                        consumedEvents.accept(nextEvent);
                    }
                    else {
                        ignoredEvents.accept(nextEvent);
                    }
                }
            }
            if (eventsRemaining > 0) {
                throw new TimeoutException(
                        "Received " + (eventCount - eventsRemaining) + " of " + eventCount + " in " + timeoutInMillis + "ms");
            }
        }

        /**
         * Blocks until the listener has seen the specified number of events with the given type, or until the default timeout
         * has passed.
         *
         * @param eventCount the number of events
         * @param type the type of event
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void consume(int eventCount, EventType type) throws TimeoutException {
            consume(eventCount, type, defaultTimeoutInMillis);
        }

        /**
         * Blocks until the listener has seen the specified number of events with the given type, or until the specified time
         * has passed.
         *
         * @param eventCount the number of events
         * @param type the type of event
         * @param timeoutMillis the maximum amount of time in milliseconds that this method should block
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void consume(int eventCount, EventType type, long timeoutMillis) throws TimeoutException {
            consume(eventCount, defaultTimeoutInMillis, event -> {
                EventHeader header = event.getHeader();
                EventType eventType = header == null ? null : header.getEventType();
                return type.equals(eventType);
            });
        }

        /**
         * Blocks until the listener has seen the specified number of events with the given type, or until the default timeout
         * has passed.
         *
         * @param eventCount the number of events
         * @param eventDataClass the EventData subclass
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void consume(int eventCount, Class<? extends EventData> eventDataClass) throws TimeoutException {
            consume(eventCount, eventDataClass, defaultTimeoutInMillis);
        }

        /**
         * Blocks until the listener has seen the specified number of events with event data matching the specified class,
         * or until the specified time has passed.
         *
         * @param eventCount the number of events
         * @param eventDataClass the EventData subclass
         * @param timeoutMillis the maximum amount of time in milliseconds that this method should block
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void consume(int eventCount, Class<? extends EventData> eventDataClass, long timeoutMillis) throws TimeoutException {
            consume(eventCount, defaultTimeoutInMillis, event -> {
                EventData data = event.getData();
                return data != null && data.getClass().equals(eventDataClass);
            });
        }

        /**
         * Clear the queue.
         */
        public void reset() {
            queue.clear();
        }
    }

    protected static class TraceLifecycleListener implements LifecycleListener {

        @Override
        public void onDisconnect(BinaryLogClient client) {
            LOGGER.debug("Client disconnected");
        }

        @Override
        public void onConnect(BinaryLogClient client) {
            LOGGER.debug("Client connected");
        }

        @Override
        public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
            LOGGER.warn("Client communication failure", ex);
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
            LOGGER.error("Client received event deserialization failure", ex);
        }
    }
}

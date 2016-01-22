/*
 * Copyright 2015 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.mysql.ingest;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.TestDatabase;
import io.debezium.mysql.MySQLConnection;

public class ReadBinLogIT {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ReadBinLogIT.class);
    protected static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(3);

    private static final class AnyValue implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static final Serializable ANY_OBJECT = new AnyValue();

    private JdbcConfiguration config;
    private EventCounters counters;
    private BinaryLogClient client;
    private MySQLConnection conn;
    private List<Event> events = new ArrayList<>();

    @Before
    public void beforeEach() throws TimeoutException, IOException, SQLException, InterruptedException {
        events.clear();

        config = TestDatabase.buildTestConfig().withDatabase("readbinlog_test").build();

        // Connect the normal SQL client ...
        conn = new MySQLConnection(config);
        conn.connect();

        // Connect the bin log client ...
        counters = new EventCounters();
        client = new BinaryLogClient(config.getHostname(), config.getPort(), "replicator", "replpass");
        client.setServerId(client.getServerId() - 1); // avoid clashes between BinaryLogClient instances
        client.setKeepAlive(false);
        client.registerEventListener(this::logEvent);
        client.registerEventListener(counters);
        client.registerEventListener(this::recordEvent);
        client.registerLifecycleListener(new TraceLifecycleListener());
        client.connect(DEFAULT_TIMEOUT); // does not block

        // Set up the table as one transaction and wait to see the events ...
        conn.execute("DROP TABLE IF EXISTS person",
                     "CREATE TABLE person (" +
                             "  name VARCHAR(255) primary key," +
                             "  age INTEGER NULL DEFAULT 10," +
                             "  createdAt DATETIME NULL DEFAULT CURRENT_TIMESTAMP," +
                             "  updatedAt DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                             ")");

        counters.waitFor(2, EventType.QUERY, DEFAULT_TIMEOUT);
        counters.reset();
    }

    @After
    public void afterEach() throws IOException, SQLException {
        events.clear();
        try {
            if (client != null) client.disconnect();
        } finally {
            client = null;
            try {
                if (conn != null) conn.close();
            } finally {
                conn = null;
            }
        }
    }

    @Test
    public void shouldCaptureSingleWriteUpdateDeleteEvents() throws Exception {
        // write/insert
        conn.execute("INSERT INTO person(name,age) VALUES ('Georgia',30)");
        counters.waitFor(1, WriteRowsEventData.class, DEFAULT_TIMEOUT);
        List<WriteRowsEventData> writeRowEvents = recordedEventData(WriteRowsEventData.class, 1);
        assertRows(writeRowEvents.get(0), rows().insertedRow("Georgia", 30, any(), any()));

        // update
        conn.execute("UPDATE person SET name = 'Maggie' WHERE name = 'Georgia'");
        counters.waitFor(1, UpdateRowsEventData.class, DEFAULT_TIMEOUT);
        List<UpdateRowsEventData> updateRowEvents = recordedEventData(UpdateRowsEventData.class, 1);
        assertRows(updateRowEvents.get(0),
                   rows().changeRow("Georgia", 30, any(), any()).to("Maggie", 30, any(), any()));

        // delete
        conn.execute("DELETE FROM person WHERE name = 'Maggie'");
        counters.waitFor(1, DeleteRowsEventData.class, DEFAULT_TIMEOUT);
        List<DeleteRowsEventData> deleteRowEvents = recordedEventData(DeleteRowsEventData.class, 1);
        assertRows(deleteRowEvents.get(0), rows().removedRow("Maggie", 30, any(), any()));
    }

    @Test
    public void shouldCaptureMultipleWriteUpdateDeleteEvents() throws Exception {
        // write/insert as a single transaction
        conn.execute("INSERT INTO person(name,age) VALUES ('Georgia',30)",
                     "INSERT INTO person(name,age) VALUES ('Janice',19)");
        counters.waitFor(1, QueryEventData.class, DEFAULT_TIMEOUT); // BEGIN
        counters.waitFor(1, TableMapEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(2, WriteRowsEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, XidEventData.class, DEFAULT_TIMEOUT); // COMMIT
        List<WriteRowsEventData> writeRowEvents = recordedEventData(WriteRowsEventData.class, 2);
        assertRows(writeRowEvents.get(0), rows().insertedRow("Georgia", 30, any(), any()));
        assertRows(writeRowEvents.get(1), rows().insertedRow("Janice", 19, any(), any()));
        counters.reset();

        // update as a single transaction
        conn.execute("UPDATE person SET name = 'Maggie' WHERE name = 'Georgia'",
                     "UPDATE person SET name = 'Jamie' WHERE name = 'Janice'");
        counters.waitFor(1, QueryEventData.class, DEFAULT_TIMEOUT); // BEGIN
        counters.waitFor(1, TableMapEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(2, UpdateRowsEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, XidEventData.class, DEFAULT_TIMEOUT); // COMMIT
        List<UpdateRowsEventData> updateRowEvents = recordedEventData(UpdateRowsEventData.class, 2);
        assertRows(updateRowEvents.get(0), rows().changeRow("Georgia", 30, any(), any()).to("Maggie", 30, any(), any()));
        assertRows(updateRowEvents.get(1), rows().changeRow("Janice", 19, any(), any()).to("Jamie", 19, any(), any()));
        counters.reset();

        // delete as a single transaction
        conn.execute("DELETE FROM person WHERE name = 'Maggie'",
                     "DELETE FROM person WHERE name = 'Jamie'");
        counters.waitFor(1, QueryEventData.class, DEFAULT_TIMEOUT); // BEGIN
        counters.waitFor(1, TableMapEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(2, DeleteRowsEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, XidEventData.class, DEFAULT_TIMEOUT); // COMMIT
        List<DeleteRowsEventData> deleteRowEvents = recordedEventData(DeleteRowsEventData.class, 2);
        assertRows(deleteRowEvents.get(0), rows().removedRow("Maggie", 30, any(), any()));
        assertRows(deleteRowEvents.get(1), rows().removedRow("Jamie", 19, any(), any()));
    }

    @Test
    public void shouldCaptureMultipleWriteUpdateDeletesInSingleEvents() throws Exception {
        // write/insert as a single statement/transaction
        conn.execute("INSERT INTO person(name,age) VALUES ('Georgia',30),('Janice',19)");
        counters.waitFor(1, QueryEventData.class, DEFAULT_TIMEOUT); // BEGIN
        counters.waitFor(1, TableMapEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, WriteRowsEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, XidEventData.class, DEFAULT_TIMEOUT); // COMMIT
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
        counters.waitFor(1, QueryEventData.class, DEFAULT_TIMEOUT); // BEGIN
        counters.waitFor(1, TableMapEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, UpdateRowsEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, XidEventData.class, DEFAULT_TIMEOUT); // COMMIT
        List<UpdateRowsEventData> updateRowEvents = recordedEventData(UpdateRowsEventData.class, 1);
        assertRows(updateRowEvents.get(0), rows().changeRow("Georgia", 30, any(), any()).to("Maggie", 30, any(), any())
                                                 .changeRow("Janice", 19, any(), any()).to("Jamie", 19, any(), any()));
        counters.reset();

        // delete as a single statement/transaction
        conn.execute("DELETE FROM person WHERE name IN ('Maggie','Jamie')");
        counters.waitFor(1, QueryEventData.class, DEFAULT_TIMEOUT); // BEGIN
        counters.waitFor(1, TableMapEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, DeleteRowsEventData.class, DEFAULT_TIMEOUT);
        counters.waitFor(1, XidEventData.class, DEFAULT_TIMEOUT); // COMMIT
        List<DeleteRowsEventData> deleteRowEvents = recordedEventData(DeleteRowsEventData.class, 1);
        assertRows(deleteRowEvents.get(0), rows().removedRow("Maggie", 30, any(), any())
                                                 .removedRow("Jamie", 19, any(), any()));
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

    protected void logEvent(Event event) {
        LOGGER.info("Received event: " + event);
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

    public static interface UpdateBuilder {
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
                if (expectedValues[i] instanceof AnyValue) actualValuesCopy[i] = expectedValues[i];
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

    protected static class EventCounters implements EventListener {
        /*
         * VariableLatch instances count down when receiving an event, and thus are negative. When callers wait for a specified
         * number of events to occur, the latch's count is incremented by the expected count. If the latch's resulting count is
         * less than or equal to 0, then the caller does not wait.
         */
        private final ConcurrentMap<EventType, AtomicInteger> counterByType = new ConcurrentHashMap<>();
        private final ConcurrentMap<Class<? extends EventData>, AtomicInteger> counterByDataClass = new ConcurrentHashMap<>();

        @Override
        public void onEvent(Event event) {
            counterByType.compute(event.getHeader().getEventType(), this::increment);
            EventData data = event.getData();
            if (data != null) {
                counterByDataClass.compute(data.getClass(), this::increment);
            }
        }

        protected <K> AtomicInteger increment(K key, AtomicInteger counter) {
            if (counter == null) return new AtomicInteger(1);
            synchronized (counter) {
                counter.incrementAndGet();
                counter.notify();
            }
            return counter;
        }

        /**
         * Blocks until the listener has seen the specified number of events with the given type.
         * 
         * @param eventCount the number of events
         * @param type the type of event
         * @param timeoutMillis the maximum amount of time in milliseconds that this method should block
         * @throws InterruptedException if the thread was interrupted while waiting
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void waitFor(int eventCount, EventType type, long timeoutMillis) throws InterruptedException, TimeoutException {
            waitFor(type.name(), () -> counterByType.get(type), eventCount, timeoutMillis);
        }

        /**
         * Blocks until the listener has seen the specified number of events with the given type.
         * 
         * @param eventCount the number of events
         * @param eventDataClass the EventData subclass
         * @param timeoutMillis the maximum amount of time in milliseconds that this method should block
         * @throws InterruptedException if the thread was interrupted while waiting
         * @throws TimeoutException if the waiting timed out before the expected number of events were received
         */
        public void waitFor(int eventCount, Class<? extends EventData> eventDataClass, long timeoutMillis)
                throws InterruptedException, TimeoutException {
            waitFor(eventDataClass.getSimpleName(), () -> counterByDataClass.get(eventDataClass), eventCount, timeoutMillis);
        }

        private void waitFor(String eventTypeName, Supplier<AtomicInteger> counterGetter, int eventCount, long timeoutMillis)
                throws InterruptedException, TimeoutException {
            // Get the counter, and prepare for it to be null ...
            AtomicInteger counter = null;
            long stopTime = System.currentTimeMillis() + timeoutMillis;
            do {
                counter = counterGetter.get();
            } while (counter == null && System.currentTimeMillis() <= stopTime);
            if (counter == null) {
                // Did not even find a counter in this timeframe ...
                throw new TimeoutException("Timed out while waiting for " + eventCount + " " + eventTypeName + " events");
            }
            synchronized (counter) {
                counter.addAndGet(-eventCount);
                if (counter.get() != 0) {
                    counter.wait(timeoutMillis);
                    if (counter.get() != 0) {
                        throw new TimeoutException("Timed out while waiting for " + eventCount + " " + eventTypeName + " events");
                    }
                }
            }
        }

        /**
         * Clear all counters.
         */
        public void reset() {
            counterByDataClass.clear();
            counterByType.clear();
        }
    }

    protected static class TraceLifecycleListener implements LifecycleListener {

        @Override
        public void onDisconnect(BinaryLogClient client) {
            LOGGER.info("Client disconnected");
        }

        @Override
        public void onConnect(BinaryLogClient client) {
            LOGGER.info("Client connected");
        }

        @Override
        public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
            LOGGER.error("Client communication failure", ex);
        }

        @Override
        public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
            LOGGER.error("Client received event deserialization failure", ex);
        }
    }
}

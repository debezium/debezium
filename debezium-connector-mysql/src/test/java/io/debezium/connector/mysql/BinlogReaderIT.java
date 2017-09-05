/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class BinlogReaderIT {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-binlog.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = new UniqueDatabase("logical_server_name", "connector_test_ro")
            .withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;
    private MySqlTaskContext context;
    private BinlogReader reader;
    private KeyValueStore store;
    private SchemaChangeHistory schemaChanges;

    @Before
    public void beforeEach() {
        Testing.Files.delete(DB_HISTORY_PATH);
        DATABASE.createAndInitialize();
        this.store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        this.schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
    }

    @After
    public void afterEach() {
        if (reader != null) {
            try {
                reader.stop();
            } finally {
                if (context != null) {
                    try {
                        context.shutdown();
                    } finally {
                        context = null;
                        Testing.Files.delete(DB_HISTORY_PATH);
                    }
                }
            }
        }
    }

    protected int consumeAtLeast(int minNumber) throws InterruptedException {
        return consumeAtLeast(minNumber, 20, TimeUnit.SECONDS);
    }

    protected int consumeAtLeast(int minNumber, long timeout, TimeUnit unit) throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        List<SourceRecord> records = null;
        long startTime = System.currentTimeMillis();
        while (counter.get() < minNumber && (System.currentTimeMillis() - startTime) < unit.toMillis(timeout)) {
            records = reader.poll();
            if (records != null) {
                records.forEach(record -> {
                    counter.incrementAndGet();
                    VerifyRecord.isValid(record);
                    store.add(record);
                    schemaChanges.add(record);
                });
                Testing.print("" + counter.get() + " records");
            }
        }
        return counter.get();
    }

    protected Configuration.Builder simpleConfig() {
        return DATABASE.defaultConfig()
                            .with(MySqlConnectorConfig.USER, "replicator")
                            .with(MySqlConnectorConfig.PASSWORD, "replpass")
                            .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config);
        context.start();
        context.source().setBinlogStartPoint("",0L); // start from beginning
        context.initializeHistory();
        reader = new BinlogReader("binlog", context);

        // Start reading the binlog ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        int expected = 9 + 9 + 4 + 5; // only the inserts for our 4 tables in this database
        int consumed = consumeAtLeast(expected);
        assertThat(consumed).isGreaterThanOrEqualTo(expected);

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(4);
        Collection products = store.collection(DATABASE.getDatabaseName(), "products");
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DATABASE.getDatabaseName(), "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DATABASE.getDatabaseName(), "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DATABASE.getDatabaseName(), "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(5);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(0);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseWithSchemaChanges() throws Exception {
        config = simpleConfig().with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true).build();
        context = new MySqlTaskContext(config);
        context.start();
        context.source().setBinlogStartPoint("",0L); // start from beginning
        context.initializeHistory();
        reader = new BinlogReader("binlog", context);

        // Start reading the binlog ...
        reader.start();

        // Poll for records ...
        // Testing.Print.enable();
        int expectedSchemaChangeCount = 4 + 2; // 4 tables plus 2 alters
        int expected = (9 + 9 + 4 + 5) + expectedSchemaChangeCount; // only the inserts for our 4 tables in this database, plus
                                                                    // schema changes
        int consumed = consumeAtLeast(expected);
        assertThat(consumed).isGreaterThanOrEqualTo(expected);

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(expectedSchemaChangeCount);

        // Check the records via the store ...
        assertThat(store.collectionCount()).isEqualTo(4);
        Collection products = store.collection(DATABASE.getDatabaseName(), "products");
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DATABASE.getDatabaseName(), "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DATABASE.getDatabaseName(), "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DATABASE.getDatabaseName(), "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(5);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(0);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);
    }

    @Test
    @FixFor( "DBZ-183" )
    public void shouldHandleTimestampTimezones() throws Exception {
        final UniqueDatabase REGRESSION_DATABASE = new UniqueDatabase("logical_server_name", "regression_test")
                .withDbHistoryPath(DB_HISTORY_PATH);
        REGRESSION_DATABASE.createAndInitialize();

        String tableName = "dbz_85_fractest";
        config = simpleConfig().with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                               .with(MySqlConnectorConfig.DATABASE_WHITELIST, REGRESSION_DATABASE.getDatabaseName())
                               .with(MySqlConnectorConfig.TABLE_WHITELIST, REGRESSION_DATABASE.qualifiedTableName(tableName))
                               .build();
        context = new MySqlTaskContext(config);
        context.start();
        context.source().setBinlogStartPoint("",0L); // start from beginning
        context.initializeHistory();
        reader = new BinlogReader("binlog", context);

        // Start reading the binlog ...
        reader.start();

        int expectedChanges = 1; // only 1 insert

        consumeAtLeast(expectedChanges);

        // Check the records via the store ...
        List<SourceRecord> sourceRecords = store.sourceRecords();
        assertThat(sourceRecords.size()).isEqualTo(1);
        ZonedDateTime expectedTimestamp = ZonedDateTime.of(LocalDateTime.parse("2014-09-08T17:51:04.780"),
                                                           ZoneId.systemDefault());
        String expectedTimestampString = expectedTimestamp.format(ZonedTimestamp.FORMATTER);
        SourceRecord sourceRecord = sourceRecords.get(0);
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        String actualTimestampString = after.getString("c4");
        assertThat(actualTimestampString).isEqualTo(expectedTimestampString);
    }
}

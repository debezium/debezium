/*
 * Copyright Debezium Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.config.Configuration;
import io.debezium.data.KeyValueStore;
import io.debezium.data.KeyValueStore.Collection;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 *
 */
public class BinlogReaderIT {

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-binlog.txt").toAbsolutePath();
    private static final String DB_NAME = "connector_test_ro";
    private static final String LOGICAL_NAME = "logical_server_name";

    private Configuration config;
    private MySqlTaskContext context;
    private BinlogReader reader;
    private KeyValueStore store;
    private SchemaChangeHistory schemaChanges;

    @Before
    public void beforeEach() {
        Testing.Files.delete(DB_HISTORY_PATH);
        this.store = KeyValueStore.createForTopicsBeginningWith(LOGICAL_NAME + ".");
        this.schemaChanges = new SchemaChangeHistory(LOGICAL_NAME);
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
        String hostname = System.getProperty("database.hostname");
        String port = System.getProperty("database.port");
        assertThat(hostname).isNotNull();
        assertThat(port).isNotNull();
        return Configuration.create()
                            .with(MySqlConnectorConfig.HOSTNAME, hostname)
                            .with(MySqlConnectorConfig.PORT, port)
                            .with(MySqlConnectorConfig.USER, "replicator")
                            .with(MySqlConnectorConfig.PASSWORD, "replpass")
                            .with(MySqlConnectorConfig.SERVER_ID, 18911)
                            .with(MySqlConnectorConfig.SERVER_NAME, LOGICAL_NAME)
                            .with(MySqlConnectorConfig.POLL_INTERVAL_MS, 10)
                            .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                            .with(MySqlConnectorConfig.DATABASE_WHITELIST, DB_NAME)
                            .with(MySqlConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                            .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH)
                            .with("database.useSSL", false); // eliminates MySQL driver warning about SSL connections
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabase() throws Exception {
        config = simpleConfig().build();
        context = new MySqlTaskContext(config);
        context.start();
        context.source().setBinlogStartPoint("",0L); // start from beginning
        reader = new BinlogReader(context);

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
        Collection products = store.collection(DB_NAME, "products");
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DB_NAME, "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DB_NAME, "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DB_NAME, "orders");
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
        reader = new BinlogReader(context);

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
        Collection products = store.collection(DB_NAME, "products");
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection products_on_hand = store.collection(DB_NAME, "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection customers = store.collection(DB_NAME, "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        Collection orders = store.collection(DB_NAME, "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(5);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(0);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);
    }
}

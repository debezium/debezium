/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.config.Field;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogSnapshotSourceIT;
import io.debezium.data.KeyValueStore;
import io.debezium.data.SchemaChangeHistory;
import io.debezium.data.VerifyRecord;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author Randall Hauch
 *
 */
public class SnapshotSourceIT extends BinlogSnapshotSourceIT<MySqlConnector> implements MySqlCommon {

    @Test
    public void snapshotWithBackupLocksShouldNotWaitForReads() throws Exception {
        config = simpleConfig()
                .with(MySqlConnectorConfig.USER, "cloud")
                .with(MySqlConnectorConfig.PASSWORD, "cloudpass")
                .with("jdbc.creds.provider.user", "cloud")
                .with("jdbc.creds.provider.password", "cloudpass")
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.MINIMAL_PERCONA)
                .build();

        if (!isPerconaServer()) {
            return; // Skip these tests for non-Percona flavours of MySQL
        }

        final MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());
        final JdbcConnection connection = db.connect();
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    connection.executeWithoutCommitting("SELECT *, SLEEP(20) FROM products_on_hand WHERE product_id=101");
                    latch.countDown();
                }
                catch (Exception e) {
                    // Do nothing.
                }
            }
        };
        t.start();

        latch.await(10, TimeUnit.SECONDS);
        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        final int recordCount = 9 + 9 + 4 + 5 + 1;
        SourceRecords sourceRecords = consumeRecordsByTopic(recordCount);
        assertThat(sourceRecords.allRecordsInOrder()).hasSize(recordCount);
        connection.connection().close();
    }

    @Test
    public void shouldCreateSnapshotOfSingleDatabaseUsingInsertEvents() throws Exception {
        config = simpleConfig()
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, "connector_(.*)_" + DATABASE.getIdentifier())
                .with("transforms", "snapshotasinsert")
                .with("transforms.snapshotasinsert.type", "io.debezium.connector.mysql.transforms.ReadToInsertEvent")
                .build();

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        // Poll for records ...
        // Testing.Print.enable();
        KeyValueStore store = KeyValueStore.createForTopicsBeginningWith(DATABASE.getServerName() + ".");
        SchemaChangeHistory schemaChanges = new SchemaChangeHistory(DATABASE.getServerName());
        SourceRecords sourceRecords = consumeRecordsByTopic(2 * (9 + 9 + 4 + 5) + 1);
        sourceRecords.allRecordsInOrder().forEach(record -> {
            VerifyRecord.isValid(record);
            VerifyRecord.hasNoSourceQuery(record);
            store.add(record);
            schemaChanges.add(record);
        });

        // There should be no schema changes ...
        assertThat(schemaChanges.recordCount()).isEqualTo(0);

        // Check the records via the store ...
        assertThat(store.databases()).containsOnly(DATABASE.getDatabaseName(), OTHER_DATABASE.getDatabaseName()); // 2 databases
        assertThat(store.collectionCount()).isEqualTo(9); // 2 databases

        KeyValueStore.Collection products = store.collection(DATABASE.getDatabaseName(), productsTableName());
        assertThat(products.numberOfCreates()).isEqualTo(9);
        assertThat(products.numberOfUpdates()).isEqualTo(0);
        assertThat(products.numberOfDeletes()).isEqualTo(0);
        assertThat(products.numberOfReads()).isEqualTo(0);
        assertThat(products.numberOfTombstones()).isEqualTo(0);
        assertThat(products.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products.numberOfValueSchemaChanges()).isEqualTo(1);

        KeyValueStore.Collection products_on_hand = store.collection(DATABASE.getDatabaseName(), "products_on_hand");
        assertThat(products_on_hand.numberOfCreates()).isEqualTo(9);
        assertThat(products_on_hand.numberOfUpdates()).isEqualTo(0);
        assertThat(products_on_hand.numberOfDeletes()).isEqualTo(0);
        assertThat(products_on_hand.numberOfReads()).isEqualTo(0);
        assertThat(products_on_hand.numberOfTombstones()).isEqualTo(0);
        assertThat(products_on_hand.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(products_on_hand.numberOfValueSchemaChanges()).isEqualTo(1);

        KeyValueStore.Collection customers = store.collection(DATABASE.getDatabaseName(), "customers");
        assertThat(customers.numberOfCreates()).isEqualTo(4);
        assertThat(customers.numberOfUpdates()).isEqualTo(0);
        assertThat(customers.numberOfDeletes()).isEqualTo(0);
        assertThat(customers.numberOfReads()).isEqualTo(0);
        assertThat(customers.numberOfTombstones()).isEqualTo(0);
        assertThat(customers.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(customers.numberOfValueSchemaChanges()).isEqualTo(1);

        KeyValueStore.Collection orders = store.collection(DATABASE.getDatabaseName(), "orders");
        assertThat(orders.numberOfCreates()).isEqualTo(5);
        assertThat(orders.numberOfUpdates()).isEqualTo(0);
        assertThat(orders.numberOfDeletes()).isEqualTo(0);
        assertThat(orders.numberOfReads()).isEqualTo(0);
        assertThat(orders.numberOfTombstones()).isEqualTo(0);
        assertThat(orders.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(orders.numberOfValueSchemaChanges()).isEqualTo(1);

        KeyValueStore.Collection timetest = store.collection(DATABASE.getDatabaseName(), "dbz_342_timetest");
        assertThat(timetest.numberOfCreates()).isEqualTo(1);
        assertThat(timetest.numberOfUpdates()).isEqualTo(0);
        assertThat(timetest.numberOfDeletes()).isEqualTo(0);
        assertThat(timetest.numberOfReads()).isEqualTo(0);
        assertThat(timetest.numberOfTombstones()).isEqualTo(0);
        assertThat(timetest.numberOfKeySchemaChanges()).isEqualTo(1);
        assertThat(timetest.numberOfValueSchemaChanges()).isEqualTo(1);
        final List<Struct> timerecords = new ArrayList<>();
        timetest.forEach(val -> {
            timerecords.add(((Struct) val.value()).getStruct("after"));
        });
        Struct after = timerecords.get(0);
        String expected = isMariaDb() ? "PT517H51M04.77S" : "PT517H51M04.78S";
        assertThat(after.get("c1")).isEqualTo(toMicroSeconds(expected));
        assertThat(after.get("c2")).isEqualTo(toMicroSeconds("-PT13H14M50S"));
        assertThat(after.get("c3")).isEqualTo(toMicroSeconds("-PT733H0M0.001S"));
        assertThat(after.get("c4")).isEqualTo(toMicroSeconds("-PT1H59M59.001S"));
        assertThat(after.get("c5")).isEqualTo(toMicroSeconds("-PT838H59M58.999999S"));
        assertThat(after.get("c6")).isEqualTo(toMicroSeconds("-PT00H20M38.000000S"));
        assertThat(after.get("c7")).isEqualTo(toMicroSeconds("-PT01H01M01.000001S"));
        assertThat(after.get("c8")).isEqualTo(toMicroSeconds("-PT01H01M01.000000S"));
        assertThat(after.get("c9")).isEqualTo(toMicroSeconds("-PT01H01M00.000000S"));
        assertThat(after.get("c10")).isEqualTo(toMicroSeconds("-PT01H00M00.000000S"));
        assertThat(after.get("c11")).isEqualTo(toMicroSeconds("-PT00H00M00.000000S"));
    }

    @Override
    protected Field getSnapshotLockingModeField() {
        return MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE;
    }

    @Override
    protected String getSnapshotLockingModeMinimal() {
        return MySqlConnectorConfig.SnapshotLockingMode.MINIMAL.getValue();
    }

    @Override
    protected String getSnapshotLockingModeNone() {
        return MySqlConnectorConfig.SnapshotLockingMode.NONE.getValue();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

public class TransactionMetadataIT extends AbstractConnectorTest {

    private static final String PRODUCT_INSERT_STMT = "INSERT INTO products (name, description, weight) VALUES ('robot', 'Toy robot', 1.304);";
    private static final String CUSTOMER_INSERT_STMT_1 = "INSERT INTO customers (first_name, last_name, email) VALUES ('Nitin', 'Agarwal', 'test1@abc.com' ); ";
    private static final String CUSTOMER_INSERT_STMT_2 = "INSERT INTO customers (first_name, last_name, email) VALUES ('Rajesh', 'Agarwal', 'test2@abc.com' ); ";
    private static final String ORDER_INSERT_STMT = "INSERT INTO orders (order_date, purchaser, quantity, product_id) VALUES ('2016-01-16', 1001, 1, 1); ";

    private static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-tm.txt").toAbsolutePath();

    private static final String SERVER_NAME = "tm_test";
    private final UniqueDatabase DATABASE = new UniqueDatabase(SERVER_NAME, "transaction_metadata_test").withDbHistoryPath(DB_HISTORY_PATH);

    private Configuration config;

    @Before
    public void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Testing.Files.delete(DB_HISTORY_PATH);
    }

    @After
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Testing.Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    public void transactionMetadataEnabled() throws InterruptedException, SQLException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(MySqlConnector.IMPLEMENTATION_PROP, "new")
                .build();

        start(MySqlConnector.class, config);

        Testing.Debug.enable();
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.setAutoCommit(false);
                connection.execute(CUSTOMER_INSERT_STMT_1, PRODUCT_INSERT_STMT, ORDER_INSERT_STMT, CUSTOMER_INSERT_STMT_2);
                connection.commit();
            }
        }
        String txId = null;
        List<SourceRecord> allRecords = new ArrayList<>();
        // read records until the transaction is found
        for (int i = 0; txId == null && i < 50; i++) {
            List<SourceRecord> records = consumeRecordsByTopic(100).allRecordsInOrder();
            txId = getTxId(records);
            allRecords.addAll(records);
        }
        assertNotNull("Failed to find the transaction", txId);
        int beginIndex = findFirstEvent(allRecords, txId);
        if (allRecords.size() < beginIndex + 6) {
            allRecords.addAll(consumeRecordsByTopic(6).allRecordsInOrder());
        }
        List<SourceRecord> transactionRecords = allRecords.subList(beginIndex, beginIndex + 1 + 4 + 1);
        assertFalse(transactionRecords.isEmpty());
        // BEGIN + 4 INSERT + END
        assertEquals(1 + 4 + 1, transactionRecords.size());
        String databaseName = DATABASE.getDatabaseName();
        String beginTxId = assertBeginTransaction(transactionRecords.get(0));
        assertEquals(txId, beginTxId);
        assertEndTransaction(transactionRecords.get(5), txId, 4, Collect.hashMapOf(databaseName + ".products", 1,
                databaseName + ".customers", 2,
                databaseName + ".orders", 1));
    }

    @Test
    @FixFor("DBZ-4077")
    public void shouldUseConfiguredTransactionTopicName() throws InterruptedException, SQLException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(MySqlConnector.IMPLEMENTATION_PROP, "new")
                .with(MySqlConnectorConfig.TRANSACTION_TOPIC, "tx.of.${database.server.name}")
                .build();

        start(MySqlConnector.class, config);

        // Testing.Debug.enable();
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.setAutoCommit(false);
                connection.execute(CUSTOMER_INSERT_STMT_1, PRODUCT_INSERT_STMT, ORDER_INSERT_STMT, CUSTOMER_INSERT_STMT_2);
                connection.commit();
            }
        }

        // TX BEGIN + 4 changes + TX END
        SourceRecords records = consumeRecordsByTopic(1 + 4 + 1);
        List<SourceRecord> txnEvents = records.recordsForTopic("tx.of." + DATABASE.getServerName());
        assertThat(txnEvents).hasSize(2);
    }

    @Test
    @FixFor("DBZ-4077")
    public void shouldUseConfiguredTransactionTopicNameWithoutServerName() throws InterruptedException, SQLException {
        config = DATABASE.defaultConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(MySqlConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(MySqlConnector.IMPLEMENTATION_PROP, "new")
                .with(MySqlConnectorConfig.TRANSACTION_TOPIC, "mytransactions")
                .build();

        start(MySqlConnector.class, config);

        // Testing.Debug.enable();
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());
        try (MySqlTestConnection db = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
            try (JdbcConnection connection = db.connect()) {
                connection.setAutoCommit(false);
                connection.execute(CUSTOMER_INSERT_STMT_1, PRODUCT_INSERT_STMT, ORDER_INSERT_STMT, CUSTOMER_INSERT_STMT_2);
                connection.commit();
            }
        }

        // TX BEGIN + 4 changes + TX END
        SourceRecords records = consumeRecordsByTopic(1 + 4 + 1);
        List<SourceRecord> txnEvents = records.recordsForTopic("mytransactions");
        assertThat(txnEvents).hasSize(2);
    }

    private String getTxId(List<SourceRecord> records) {
        Optional<Struct> product = records.stream()
                .map(sr -> (Struct) sr.value())
                .filter(sr -> sr.schema().field("source") != null)
                .filter(sr -> sr.getStruct("source").getString("table").equals("products"))
                .filter(s -> s.getStruct("after").getString("description").equals("Toy robot"))
                .findFirst();
        return product.map(struct -> (String) struct.getStruct("transaction").get("id")).orElse(null);
    }

    private int findFirstEvent(List<SourceRecord> records, String txId) {
        int i = 0;
        for (SourceRecord sr : records) {
            if (((Struct) sr.value()).getString("id").equals(txId)) {
                return i;
            }
            i++;
        }
        return -1;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Integration test to check transaction metadata.
 *
 * @author Jiri Pechanec
 */
public class TransactionMetadataIT extends AbstractAsyncEngineConnectorTest {

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.customer");
        TestHelper.dropTable(connection, "debezium.orders");

        String ddl = "create table debezium.customer (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        ddl = "create table debezium.orders (" +
                " id number(6) not null primary key, " +
                " order_date date not null, " +
                " purchaser number(4) not null, " +
                " quantity number(4) not null, " +
                " product_id number(4) not null" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.orders to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            TestHelper.dropTable(connection, "debezium.orders");
            TestHelper.dropTable(connection, "debezium.customer");
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        connection.execute("delete from debezium.customer");
        connection.execute("delete from debezium.orders");
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @Test
    public void transactionMetadata() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER,DEBEZIUM\\.ORDERS")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(OracleConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(OracleConnectorConfig.LOG_MINING_STRATEGY, OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.executeWithoutCommitting("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
        connection.executeWithoutCommitting("INSERT INTO debezium.orders VALUES (1, TO_DATE('2021-02-01', 'yyyy-mm-dd'), 1001, 1, 102)");
        connection.execute("COMMIT");

        // TX BEGIN, insert x2, TX END
        final int expectedRecordCount = 1 + 2 + 1;
        List<SourceRecord> records = consumeRecordsByTopic(expectedRecordCount).allRecordsInOrder();
        assertThat(records).hasSize(expectedRecordCount);

        // TX Begin
        SourceRecord record = records.get(0);
        String expectedTxId = assertBeginTransaction(record);

        // Insert customer
        record = records.get(1);
        VerifyRecord.isValidInsert(record, "ID", 1);
        Struct after = (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertRecordTransactionMetadata(record, expectedTxId, 1, 1);

        // Insert orders
        record = records.get(2);
        VerifyRecord.isValidInsert(record, "ID", 1);
        after = (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("ORDER_DATE")).isEqualTo(1612137600000L);
        assertThat(after.get("PURCHASER")).isEqualTo((short) 1001);
        assertThat(after.get("QUANTITY")).isEqualTo((short) 1);
        assertThat(after.get("PRODUCT_ID")).isEqualTo((short) 102);
        assertRecordTransactionMetadata(record, expectedTxId, 2, 1);

        // TX End
        record = records.get(3);

        final String dbName = TestHelper.getDatabaseName();
        assertEndTransaction(record, expectedTxId, 2, Collect.hashMapOf(dbName + ".DEBEZIUM.CUSTOMER", 1, dbName + ".DEBEZIUM.ORDERS", 1));
    }

    @Test
    @FixFor("DBZ-3090")
    public void transactionMetadataMultipleTransactions() throws Exception {
        try (OracleConnection secondaryConn = TestHelper.testConnection()) {

            final String dbName = TestHelper.getDatabaseName();

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER,DEBEZIUM\\.ORDERS")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                    .with(OracleConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                    .with(OracleConnectorConfig.LOG_MINING_STRATEGY, OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG)
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Create multiple transaction commits, notice commit order
            connection.executeWithoutCommitting("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");
            connection.executeWithoutCommitting("INSERT INTO debezium.orders VALUES (2, TO_DATE('2021-02-01', 'yyyy-mm-dd'), 1001, 2, 102)");
            secondaryConn.executeWithoutCommitting("INSERT INTO debezium.orders VALUES (1, TO_DATE('2021-02-01', 'yyyy-mm-dd'), 1001, 1, 102)");
            secondaryConn.execute("COMMIT");
            connection.execute("COMMIT");

            // 2 TX BEGIN, 3 insert, 2 TX END
            final int expectedRecordCount = 2 + 3 + 2;
            List<SourceRecord> records = consumeRecordsByTopic(expectedRecordCount).allRecordsInOrder();
            assertThat(records).hasSize(expectedRecordCount);

            // TX Begin
            SourceRecord record = records.get(0);
            String expectedTxId = assertBeginTransaction(record);

            // Insert orders (secondaryConn commit)
            record = records.get(1);
            VerifyRecord.isValidInsert(record, "ID", 1);
            Struct after = (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("ORDER_DATE")).isEqualTo(1612137600000L);
            assertThat(after.get("PURCHASER")).isEqualTo((short) 1001);
            assertThat(after.get("QUANTITY")).isEqualTo((short) 1);
            assertThat(after.get("PRODUCT_ID")).isEqualTo((short) 102);
            assertRecordTransactionMetadata(record, expectedTxId, 1, 1);

            // TX End
            record = records.get(2);
            assertEndTransaction(record, expectedTxId, 1, Collect.hashMapOf(dbName + ".DEBEZIUM.ORDERS", 1));

            // TX Begin
            record = records.get(3);
            expectedTxId = assertBeginTransaction(record);

            // Insert customer (connection commit)
            record = records.get(4);
            VerifyRecord.isValidInsert(record, "ID", 1);
            after = (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
            assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
            assertRecordTransactionMetadata(record, expectedTxId, 1, 1);

            // Insert orders (connection commit)
            record = records.get(5);
            VerifyRecord.isValidInsert(record, "ID", 2);
            after = (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(2);
            assertThat(after.get("ORDER_DATE")).isEqualTo(1612137600000L);
            assertThat(after.get("PURCHASER")).isEqualTo((short) 1001);
            assertThat(after.get("QUANTITY")).isEqualTo((short) 2);
            assertThat(after.get("PRODUCT_ID")).isEqualTo((short) 102);
            assertRecordTransactionMetadata(record, expectedTxId, 2, 1);

            // TX End
            record = records.get(6);
            assertEndTransaction(record, expectedTxId, 2, Collect.hashMapOf(dbName + ".DEBEZIUM.CUSTOMER", 1, dbName + ".DEBEZIUM.ORDERS", 1));
        }
    }
}

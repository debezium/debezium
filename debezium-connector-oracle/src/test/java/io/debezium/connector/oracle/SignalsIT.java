/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.spi.DropTransactionAction;
import io.debezium.connector.oracle.util.OracleMetricsHelper;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * Integration test for signaling schema changes.
 *
 * @author Jiri Pechanec
 */
@SkipWhenDatabaseVersion(check = EqualityCheck.GREATER_THAN_OR_EQUAL, major = 21, reason = "Not compatible, schema changes cause unexpected 'COL#' columns")
public class SignalsIT extends AbstractAsyncEngineConnectorTest {

    private static OracleConnection connection;

    @Rule
    public SkipTestRule skipRule = new SkipTestRule();

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        TestHelper.dropTable(connection, "debezium.customer");
        TestHelper.dropTable(connection, "debezium.debezium_signal");

        String ddl = "create table debezium.customer (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(1000), " +
                "  score decimal(6, 2), " +
                "  registered timestamp, " +
                "  constraint mypk primary key (id)" +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.customer to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.customer ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        ddl = "create table debezium.debezium_signal (" +
                " id varchar2(64), " +
                " type varchar2(64), " +
                " data varchar2(2048) " +
                ")";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.debezium_signal to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.debezium_signal ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        TestHelper.dropTable(connection, "debezium.debezium_signal");
        TestHelper.dropTable(connection, "debezium.customer");
    }

    @Test
    public void signalSchemaChange() throws Exception {
        // Testing.Print.enable();

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.getDatabaseName() + ".DEBEZIUM.DEBEZIUM_SIGNAL")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(OracleConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(OracleConnectorConfig.LOG_MINING_STRATEGY, OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");

        // Insert the signal record - add 'NAME' column to PK fields
        connection.execute("ALTER TABLE debezium.customer DROP CONSTRAINT mypk");
        connection.execute("ALTER TABLE debezium.customer ADD CONSTRAINT mypk PRIMARY KEY (id, name)");
        connection.execute(
                "INSERT INTO debezium.debezium_signal VALUES('1', 'schema-changes', '{\"database\": \"ORCLPDB1\", \"schema\": \"DEBEZIUM\", \"changes\":[{\"type\":\"ALTER\",\"id\":\"\\\"ORCLPDB1\\\".\\\"DEBEZIUM\\\".\\\"CUSTOMER\\\"\",\"table\":{\"defaultCharsetName\":null,\"primaryKeyColumnNames\":[\"ID\", \"NAME\"],\"columns\":[{\"name\":\"ID\",\"jdbcType\":2,\"typeName\":\"NUMBER\",\"typeExpression\":\"NUMBER\",\"charsetName\":null,\"length\":9,\"scale\":0,\"position\":1,\"optional\":false,\"autoIncremented\":false,\"generated\":false},{\"name\":\"NAME\",\"jdbcType\":12,\"typeName\":\"VARCHAR2\",\"typeExpression\":\"VARCHAR2\",\"charsetName\":null,\"length\":1000,\"position\":2,\"optional\":true,\"autoIncremented\":false,\"generated\":false},{\"name\":\"SCORE\",\"jdbcType\":2,\"typeName\":\"NUMBER\",\"typeExpression\":\"NUMBER\",\"charsetName\":null,\"length\":6,\"scale\":2,\"position\":3,\"optional\":true,\"autoIncremented\":false,\"generated\":false},{\"name\":\"REGISTERED\",\"jdbcType\":93,\"typeName\":\"TIMESTAMP(6)\",\"typeExpression\":\"TIMESTAMP(6)\",\"charsetName\":null,\"length\":6,\"position\":4,\"optional\":true,\"autoIncremented\":false,\"generated\":false}]}}]}')");

        connection.execute("INSERT INTO debezium.customer VALUES (2, 'Battle-Bug', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");

        // two schema changes, one data record, two schema changes (alters), one signal record, one schema change, one data record
        final int expected = 2 + 1 + 2 + 1 + 1 + 1;
        List<SourceRecord> records = consumeRecordsByTopic(expected).allRecordsInOrder();
        assertThat(records).hasSize(expected);

        final SourceRecord pre = records.get(0);
        final SourceRecord post = records.get(7);

        assertThat(((Struct) pre.key()).schema().fields()).hasSize(1);

        final Struct postKey = (Struct) post.key();
        assertThat(postKey.schema().fields()).hasSize(2);
        assertThat(postKey.schema().field("ID")).isNotNull();
        assertThat(postKey.schema().field("NAME")).isNotNull();

        stopConnector();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        connection.execute("INSERT INTO debezium.customer VALUES (3, 'Crazy-Frog', 1234.56, TO_DATE('2018-02-22', 'yyyy-mm-dd'))");

        // two schema changes, one data record, one signal record, one schema change, one data record
        records = consumeRecordsByTopic(1).allRecordsInOrder();
        assertThat(records).hasSize(1);

        final SourceRecord post2 = records.get(0);
        final Struct postKey2 = (Struct) post2.key();
        assertThat(postKey2.schema().fields()).hasSize(2);
        assertThat(postKey2.schema().field("ID")).isNotNull();
        assertThat(postKey2.schema().field("NAME")).isNotNull();
    }

    @Test
    @SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_BUFFERED)
    public void shouldDropTransactionViaSignal() throws Exception {
        // There isn't an easy way to track the transaction ID, so we test the negative case
        // to validate signal processing and logging work correctly.

        final LogInterceptor logInterceptor = new LogInterceptor(DropTransactionAction.class);

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SIGNAL_DATA_COLLECTION, TestHelper.getDatabaseName() + ".DEBEZIUM.DEBEZIUM_SIGNAL")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Create an uncommitted transaction in the buffer (won't be captured until committed)
        connection.executeWithoutCommitting("INSERT INTO debezium.customer VALUES (1, 'Uncommitted-Transaction', 100.00, TO_DATE('2024-01-01', 'yyyy-mm-dd'))");

        // Send drop-transaction signal with a fake transaction ID using a separate connection
        // This ensures the signal doesn't accidentally commit the uncommitted transaction above
        final String fakeTransactionId = "fake.transaction.id.12345";
        try (OracleConnection signalConnection = TestHelper.testConnection()) {
            signalConnection.execute(String.format(
                    "INSERT INTO debezium.debezium_signal VALUES('drop-tx-1', 'drop-transaction', '{\"transaction-id\": \"%s\"}')",
                    fakeTransactionId));
        }

        // Wait for signal to be processed
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> logInterceptor.containsMessage("Attempting to drop transaction '" + fakeTransactionId + "'"));

        // Verify the signal was processed and warning logged for non-existent transaction
        assertThat(logInterceptor.containsMessage("Attempting to drop transaction '" + fakeTransactionId + "'")).isTrue();
        assertThat(logInterceptor.containsWarnMessage("Transaction '" + fakeTransactionId + "' was not found")).isTrue();

        // Commit the first transaction (was uncommitted until now)
        connection.execute("COMMIT");

        // Insert a new transaction to verify connector still works after the signal
        connection.execute("INSERT INTO debezium.customer VALUES (2, 'After-Signal', 88.88, TO_DATE('2024-01-02', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        OracleMetricsHelper.waitForCurrentScnToHaveBeenSeenByConnector();

        // Should receive 2 records (the first uncommitted transaction + the After-Signal transaction)
        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> customerRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        List<SourceRecord> signalRecords = records.recordsForTopic("server1.DEBEZIUM.DEBEZIUM_SIGNAL");
        assertThat(customerRecords).hasSize(2);
        assertThat(signalRecords).hasSize(1);

        // Validate the schema and content of the first customer record
        SourceRecord firstRecord = customerRecords.get(0);
        final Struct firstKey = (Struct) firstRecord.key();
        assertThat(firstKey.schema().fields()).hasSize(1);
        assertThat(firstKey.schema().field("ID")).isNotNull();

        Struct firstValue = (Struct) firstRecord.value();
        Struct firstAfter = firstValue.getStruct("after");
        assertThat(firstAfter.get("ID")).isEqualTo(1);
        assertThat(firstAfter.getString("NAME")).isEqualTo("Uncommitted-Transaction");

        // Validate the schema and content of the second customer record
        SourceRecord secondRecord = customerRecords.get(1);
        final Struct secondKey = (Struct) secondRecord.key();
        assertThat(secondKey.schema().fields()).hasSize(1);
        assertThat(secondKey.schema().field("ID")).isNotNull();

        Struct secondValue = (Struct) secondRecord.value();
        Struct secondAfter = secondValue.getStruct("after");
        assertThat(secondAfter.get("ID")).isEqualTo(2);
        assertThat(secondAfter.getString("NAME")).isEqualTo("After-Signal");

        // Validate the signal record
        SourceRecord signalRecord = signalRecords.get(0);
        Struct signalValue = (Struct) signalRecord.value();
        Struct signalAfter = signalValue.getStruct("after");
        assertThat(signalAfter.getString("ID")).isEqualTo("drop-tx-1");
        assertThat(signalAfter.getString("TYPE")).isEqualTo("drop-transaction");
        assertThat(signalAfter.getString("DATA")).contains(fakeTransactionId);

        assertNoRecordsToConsume();
        stopConnector();
    }

}

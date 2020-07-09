/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Integration test to check transaction metadata.
 *
 * @author Jiri Pechanec
 */
public class TransactionMetadataIT extends AbstractConnectorTest {

    private static OracleConnection connection;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.customer");

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
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        connection.execute("delete from debezium.customer");
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
    }

    @Test
    @SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.LOGMINER, reason = "End transaction is not emitted")
    public void transactionMetadata() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.SCHEMA_ONLY)
                .with(OracleConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // Testing.Print.enable();
        connection.execute("INSERT INTO debezium.customer VALUES (1, 'Billie-Bob', 1234.56, TO_DATE('2018/02/22', 'yyyy-mm-dd'))");
        connection.execute("COMMIT");

        // TX BEGIN, insert, TX END
        final int expectedRecordCount = 1 + 1 + 1;
        List<SourceRecord> records = consumeRecordsByTopic(expectedRecordCount).allRecordsInOrder();
        assertThat(records).hasSize(expectedRecordCount);

        final String expectedTxId = assertBeginTransaction(records.get(0));

        // insert
        VerifyRecord.isValidInsert(records.get(1), "ID", 1);
        Struct after = (Struct) ((Struct) records.get(1).value()).get("after");
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Billie-Bob");
        assertThat(after.get("SCORE")).isEqualTo(BigDecimal.valueOf(1234.56));
        assertRecordTransactionMetadata(records.get(1), expectedTxId, 1, 1);

        assertEndTransaction(records.get(2), expectedTxId, 1, Collect.hashMapOf("ORCLPDB1.DEBEZIUM.CUSTOMER", 1));
    }
}

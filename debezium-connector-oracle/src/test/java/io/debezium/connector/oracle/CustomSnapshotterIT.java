/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

public class CustomSnapshotterIT extends AbstractAsyncEngineConnectorTest {

    private static OracleConnection connection;

    private static final String PK_FIELD = "PK";

    @BeforeClass
    public static void beforeClass() throws SQLException {

        connection = TestHelper.testConnection();

        TestHelper.dropAllTables();

        connection.execute("CREATE TABLE debezium.a (pk numeric(9,0), aa integer, primary key(pk))");
        connection.execute("GRANT SELECT ON debezium.a to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.a ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        connection.execute("CREATE TABLE debezium.b (pk numeric(9,0), aa integer, PRIMARY KEY(pk))");
        connection.execute("GRANT SELECT ON debezium.b to  " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.b ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            TestHelper.dropTable(connection, "debezium.a");
            TestHelper.dropTable(connection, "debezium.b");
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @Test
    public void shouldAllowForCustomSnapshot() throws InterruptedException, SQLException {

        connection.execute("INSERT INTO debezium.a (pk, aa) VALUES (1, 1)");
        connection.execute("INSERT INTO debezium.b (pk, aa) VALUES (1, 1)");
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.A, DEBEZIUM\\.B")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.CUSTOM.getValue())
                .with(OracleConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(OracleConnectorConfig.SNAPSHOT_QUERY_MODE, CommonConnectorConfig.SnapshotQueryMode.CUSTOM)
                .with(OracleConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .build();
        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("DEBEZIUM", "A"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("DEBEZIUM", "B"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs).isNull();

        SourceRecord record = s1recs.get(0);
        VerifyRecord.isValidRead(record, PK_FIELD, 1);

        connection.execute("INSERT INTO debezium.a (pk, aa) VALUES (2, 1)");
        connection.execute("INSERT INTO debezium.b (pk, aa) VALUES (2, 1)");
        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("DEBEZIUM", "A"));
        s2recs = actualRecords.recordsForTopic(topicName("DEBEZIUM", "B"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        record = s1recs.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);
        record = s2recs.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);
        stopConnector();

        config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.A, DEBEZIUM\\.B")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.CUSTOM.getValue())
                .with(OracleConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(OracleConnectorConfig.SNAPSHOT_QUERY_MODE, CommonConnectorConfig.SnapshotQueryMode.CUSTOM)
                .with(OracleConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .build();
        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        actualRecords = consumeRecordsByTopic(4);

        s1recs = actualRecords.recordsForTopic(topicName("DEBEZIUM", "A"));
        s2recs = actualRecords.recordsForTopic(topicName("DEBEZIUM", "B"));
        assertThat(s1recs.size()).isEqualTo(2);
        assertThat(s2recs.size()).isEqualTo(2);
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 2);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(1), PK_FIELD, 2);
    }

    private String topicName(String schema, String table) {

        return TestHelper.SERVER_NAME + "." + schema + "." + table;
    }
}

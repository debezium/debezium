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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.EqualityCheck;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
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

    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            TestHelper.dropTable(connection, "debezium.debezium_signal");
            TestHelper.dropTable(connection, "debezium.customer");
            connection.close();
        }
    }

    @Before
    public void before() throws SQLException {
        connection.execute("delete from debezium.customer");
        connection.execute("delete from debezium.debezium_signal");
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
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
}

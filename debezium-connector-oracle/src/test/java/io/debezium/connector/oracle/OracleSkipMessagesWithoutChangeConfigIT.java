/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;

/**
 * Integration tests for config skip.messages.without.change
 *
 * @author Ronak Jain
 */
public class OracleSkipMessagesWithoutChangeConfigIT extends AbstractConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnection connection;

    @Before
    public void before() {
        connection = TestHelper.testConnection();
        TestHelper.dropTable(connection, "debezium.test");
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            TestHelper.dropTable(connection, "debezium.test");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabled() throws Exception {
        String ddl = "CREATE TABLE debezium.test (" +
                "  id INT NOT NULL, white INT, black INT, PRIMARY KEY (id))";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.test TO " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TEST")
                .with(OracleConnectorConfig.COLUMN_INCLUDE_LIST, "DEBEZIUM\\.TEST\\.ID, DEBEZIUM\\.TEST\\.WHITE")
                .with(OracleConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.test VALUES (1, 1, 1)");
        connection.execute("UPDATE debezium.test SET black=2 where id = 1");
        connection.execute("UPDATE debezium.test SET white=2 where id = 1");
        connection.execute("UPDATE debezium.test SET white=3, black=3 where id = 1");

        /*
         * Total Events
         * 1,1,1 (I)
         * 1,1,2 (U) (Skipped)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("test"));
        assertThat(recordsForTopic).hasSize(3);

        Struct secondMessage = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(secondMessage.get("WHITE")).isEqualTo(new BigDecimal("2"));
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("WHITE")).isEqualTo(new BigDecimal("3"));
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldSkipEventsWithNoChangeInIncludedColumnsWhenSkipEnabledWithExcludeConfig() throws Exception {
        String ddl = "CREATE TABLE debezium.test (" +
                "  id INT NOT NULL, white INT, black INT, PRIMARY KEY (id))";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.test TO " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TEST")
                .with(OracleConnectorConfig.COLUMN_EXCLUDE_LIST, "DEBEZIUM\\.TEST\\.BLACK")
                .with(OracleConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, true)
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.test VALUES (1, 1, 1)");
        connection.execute("UPDATE debezium.test SET black=2 where id = 1");
        connection.execute("UPDATE debezium.test SET white=2 where id = 1");
        connection.execute("UPDATE debezium.test SET white=3, black=3 where id = 1");

        /*
         * Total Events
         * 1,1,1 (I)
         * 1,1,2 (U) (Skipped)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("test"));
        assertThat(recordsForTopic).hasSize(3);

        Struct secondMessage = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(secondMessage.get("WHITE")).isEqualTo(new BigDecimal("2"));
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("WHITE")).isEqualTo(new BigDecimal("3"));
    }

    @Test
    @FixFor("DBZ-2979")
    public void shouldNotSkipEventsWithNoChangeInIncludedColumnsWhenSkipDisabled() throws Exception {
        String ddl = "CREATE TABLE debezium.test (" +
                "  id INT NOT NULL, white INT, black INT, PRIMARY KEY (id))";

        connection.execute(ddl);
        connection.execute("GRANT SELECT ON debezium.test TO " + TestHelper.getConnectorUserName());
        connection.execute("ALTER TABLE debezium.test ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TEST")
                .with(OracleConnectorConfig.COLUMN_INCLUDE_LIST, "DEBEZIUM\\.TEST\\.ID, DEBEZIUM\\.TEST\\.WHITE")
                .with(OracleConnectorConfig.SKIP_MESSAGES_WITHOUT_CHANGE, false)
                .with(OracleConnectorConfig.SNAPSHOT_MODE, OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        connection.execute("INSERT INTO debezium.test VALUES (1, 1, 1)");
        connection.execute("UPDATE debezium.test SET black=2 where id = 1");
        connection.execute("UPDATE debezium.test SET white=2 where id = 1");
        connection.execute("UPDATE debezium.test SET white=3, black=3 where id = 1");

        /*
         * Total Events
         * 1,1,1 (I)
         * 1,1,2 (U)
         * 1,2,2 (U)
         * 1,3,3 (U)
         */
        SourceRecords records = consumeRecordsByTopic(4);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("test"));
        assertThat(recordsForTopic).hasSize(4);

        Struct secondMessage = ((Struct) recordsForTopic.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(secondMessage.get("WHITE")).isEqualTo(new BigDecimal("1"));
        Struct thirdMessage = ((Struct) recordsForTopic.get(2).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(thirdMessage.get("WHITE")).isEqualTo(new BigDecimal("2"));
        Struct forthMessage = ((Struct) recordsForTopic.get(3).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(forthMessage.get("WHITE")).isEqualTo(new BigDecimal("3"));
    }

    private static String topicName(String tableName) {
        return TestHelper.SERVER_NAME + ".DEBEZIUM." + tableName.toUpperCase();
    }
}

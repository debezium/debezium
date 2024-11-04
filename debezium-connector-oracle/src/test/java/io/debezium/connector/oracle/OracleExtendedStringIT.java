/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipOnDatabaseParameter;
import io.debezium.connector.oracle.junit.SkipTestDependingOnDatabaseParameterRule;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration tests for when MAX_STRING_SIZE is set to EXTENDED.
 *
 * @author Chris Cranford
 */
@SkipOnDatabaseParameter(parameterName = "max_string_size", value = "EXTENDED", matches = false, reason = "Requires max_string_size set to EXTENDED")
public class OracleExtendedStringIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public TestRule skipOnDatabaseParameter = new SkipTestDependingOnDatabaseParameterRule();

    private OracleConnection connection;

    @Before
    public void beforeEach() throws Exception {
        this.connection = TestHelper.testConnection();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() throws Exception {
        stopConnector();
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-8039")
    public void shouldStreamExtendedStringValueShorterThan4k() throws Exception {
        TestHelper.dropTable(connection, "dbz8039");
        try {
            // Creates a table with extended string support
            connection.execute("CREATE TABLE dbz8039 (id numeric(9,0) primary key, data varchar2(8000))");
            TestHelper.streamTable(connection, "dbz8039");

            final String snapshotData = RandomStringUtils.randomAlphanumeric(3000);
            connection.prepareUpdate("INSERT INTO dbz8039 (id,data) values (0,?)", ps -> ps.setString(1, snapshotData));
            connection.commit();

            final Configuration config = defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8039")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String initialData = RandomStringUtils.randomAlphanumeric(3000);
            connection.prepareUpdate("INSERT INTO dbz8039 (id,data) values (1,?)", ps -> ps.setString(1, initialData));
            connection.commit();

            final String data = RandomStringUtils.randomAlphanumeric(3000);
            connection.prepareUpdate("UPDATE dbz8039 SET data=? WHERE id=1", ps -> ps.setString(1, data));
            connection.commit();

            connection.execute("DELETE FROM dbz8039 WHERE id=1");

            List<SourceRecord> records = consumeRecordsByTopic(4).recordsForTopic(topicName("DBZ8039"));

            VerifyRecord.isValidRead(records.get(0), "ID", 0);
            assertThat(getAfter(records.get(0)).get("DATA")).isEqualTo(snapshotData);

            VerifyRecord.isValidInsert(records.get(1), "ID", 1);
            assertThat(getAfter(records.get(1)).get("DATA")).isEqualTo(initialData);

            VerifyRecord.isValidUpdate(records.get(2), "ID", 1);
            assertUnavailableValue(getBefore(records.get(2)).get("DATA"));
            assertThat(getAfter(records.get(2)).get("DATA")).isEqualTo(data);

            VerifyRecord.isValidDelete(records.get(3), "ID", 1);
            assertUnavailableValue(getBefore(records.get(3)).get("DATA"));
        }
        finally {
            TestHelper.dropTable(connection, "dbz8039");
        }
    }

    @Test
    @FixFor("DBZ-8039")
    public void shouldStreamExtendedStringValueBothShorterThanAndGreaterThan4k() throws Exception {
        TestHelper.dropTable(connection, "dbz8039");
        try {
            // Creates a table with extended string support
            connection.execute("CREATE TABLE dbz8039 (id numeric(9,0) primary key, data1 varchar2(8000), data2 varchar2(8000))");
            TestHelper.streamTable(connection, "dbz8039");

            final String snapshotData1 = RandomStringUtils.randomAlphanumeric(3000);
            final String snapshotData2 = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("INSERT INTO dbz8039 (id,data1,data2) values (0,?,?)",
                    ps -> {
                        ps.setString(1, snapshotData1);
                        ps.setString(2, snapshotData2);
                    });
            connection.commit();

            final Configuration config = defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8039")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String initialData1 = RandomStringUtils.randomAlphanumeric(3000);
            final String initialData2 = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("INSERT INTO dbz8039 (id,data1,data2) values (1,?,?)",
                    ps -> {
                        ps.setString(1, initialData1);
                        ps.setString(2, initialData2);
                    });
            connection.commit();

            final String data1 = RandomStringUtils.randomAlphanumeric(3000);
            final String data2 = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("UPDATE dbz8039 SET data1=?, data2=? WHERE id=1",
                    ps -> {
                        ps.setString(1, data1);
                        ps.setString(2, data2);
                    });
            connection.commit();

            connection.execute("DELETE FROM dbz8039 WHERE id=1");

            List<SourceRecord> records = consumeRecordsByTopic(4).recordsForTopic(topicName("DBZ8039"));

            VerifyRecord.isValidRead(records.get(0), "ID", 0);
            assertThat(getAfter(records.get(0)).get("DATA1")).isEqualTo(snapshotData1);
            assertThat(getAfter(records.get(0)).get("DATA2")).isEqualTo(snapshotData2);

            VerifyRecord.isValidInsert(records.get(1), "ID", 1);
            assertThat(getAfter(records.get(1)).get("DATA1")).isEqualTo(initialData1);
            assertThat(getAfter(records.get(1)).get("DATA2")).isEqualTo(initialData2);

            VerifyRecord.isValidUpdate(records.get(2), "ID", 1);
            assertUnavailableValue(getBefore(records.get(2)).get("DATA1"));
            assertUnavailableValue(getBefore(records.get(2)).get("DATA2"));
            assertThat(getAfter(records.get(2)).get("DATA1")).isEqualTo(data1);
            assertThat(getAfter(records.get(2)).get("DATA2")).isEqualTo(data2);

            VerifyRecord.isValidDelete(records.get(3), "ID", 1);
            assertUnavailableValue(getBefore(records.get(3)).get("DATA1"));
            assertUnavailableValue(getBefore(records.get(3)).get("DATA2"));
        }
        finally {
            TestHelper.dropTable(connection, "dbz8039");
        }
    }

    @Test
    @FixFor("DBZ-8039")
    public void shouldStreamExtendedStringColumnGreaterThan4k() throws Exception {
        TestHelper.dropTable(connection, "dbz8039");
        try {
            // Creates a table with extended string support
            connection.execute("CREATE TABLE dbz8039 (id numeric(9,0) primary key, data varchar2(8000))");
            TestHelper.streamTable(connection, "dbz8039");

            final String snapshotData = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("INSERT INTO dbz8039 (id,data) values (0,?)", ps -> ps.setString(1, snapshotData));
            connection.commit();

            final Configuration config = defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8039")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String initialData = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("INSERT INTO dbz8039 (id,data) values (1,?)", ps -> ps.setString(1, initialData));
            connection.commit();

            final String data = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("UPDATE dbz8039 SET data=? WHERE id=1", ps -> ps.setString(1, data));
            connection.commit();

            connection.execute("DELETE FROM dbz8039 WHERE id=1");

            List<SourceRecord> records = consumeRecordsByTopic(4).recordsForTopic(topicName("DBZ8039"));

            VerifyRecord.isValidRead(records.get(0), "ID", 0);
            assertThat(getAfter(records.get(0)).get("DATA")).isEqualTo(snapshotData);

            VerifyRecord.isValidInsert(records.get(1), "ID", 1);
            assertThat(getAfter(records.get(1)).get("DATA")).isEqualTo(initialData);

            VerifyRecord.isValidUpdate(records.get(2), "ID", 1);
            assertUnavailableValue(getBefore(records.get(2)).get("DATA"));
            assertThat(getAfter(records.get(2)).get("DATA")).isEqualTo(data);

            VerifyRecord.isValidDelete(records.get(3), "ID", 1);
            assertUnavailableValue(getBefore(records.get(3)).get("DATA"));
        }
        finally {
            TestHelper.dropTable(connection, "dbz8039");
        }
    }

    @Test
    @FixFor("DBZ-8039")
    public void shouldStreamMultipleExtendedStringColumnsGreaterThan4k() throws Exception {
        TestHelper.dropTable(connection, "dbz8039");
        try {
            // Creates a table with extended string support
            connection.execute("CREATE TABLE dbz8039 (id numeric(9,0) primary key, data1 varchar2(8000), data2 varchar2(8000))");
            TestHelper.streamTable(connection, "dbz8039");

            final String snapshotData1 = RandomStringUtils.randomAlphanumeric(5000);
            final String snapshotData2 = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("INSERT INTO dbz8039 (id,data1,data2) values (0,?,?)",
                    ps -> {
                        ps.setString(1, snapshotData1);
                        ps.setString(2, snapshotData2);
                    });
            connection.commit();

            final Configuration config = defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8039")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String initialData1 = RandomStringUtils.randomAlphanumeric(5000);
            final String initialData2 = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("INSERT INTO dbz8039 (id,data1,data2) values (1,?,?)",
                    ps -> {
                        ps.setString(1, initialData1);
                        ps.setString(2, initialData2);
                    });
            connection.commit();

            final String data1 = RandomStringUtils.randomAlphanumeric(5000);
            final String data2 = RandomStringUtils.randomAlphanumeric(5000);
            connection.prepareUpdate("UPDATE dbz8039 SET data1=?, data2=? WHERE id=1",
                    ps -> {
                        ps.setString(1, data1);
                        ps.setString(2, data2);
                    });
            connection.commit();

            connection.execute("DELETE FROM dbz8039 WHERE id=1");

            List<SourceRecord> records = consumeRecordsByTopic(4).recordsForTopic(topicName("DBZ8039"));

            VerifyRecord.isValidRead(records.get(0), "ID", 0);
            assertThat(getAfter(records.get(0)).get("DATA1")).isEqualTo(snapshotData1);
            assertThat(getAfter(records.get(0)).get("DATA2")).isEqualTo(snapshotData2);

            VerifyRecord.isValidInsert(records.get(1), "ID", 1);
            assertThat(getAfter(records.get(1)).get("DATA1")).isEqualTo(initialData1);
            assertThat(getAfter(records.get(1)).get("DATA2")).isEqualTo(initialData2);

            VerifyRecord.isValidUpdate(records.get(2), "ID", 1);
            assertUnavailableValue(getBefore(records.get(2)).get("DATA1"));
            assertUnavailableValue(getBefore(records.get(2)).get("DATA2"));
            assertThat(getAfter(records.get(2)).get("DATA1")).isEqualTo(data1);
            assertThat(getAfter(records.get(2)).get("DATA2")).isEqualTo(data2);

            VerifyRecord.isValidDelete(records.get(3), "ID", 1);
            assertUnavailableValue(getBefore(records.get(3)).get("DATA1"));
            assertUnavailableValue(getBefore(records.get(3)).get("DATA2"));
        }
        finally {
            TestHelper.dropTable(connection, "dbz8039");
        }
    }

    private static Configuration.Builder defaultConfig() {
        return TestHelper.defaultConfig().with(OracleConnectorConfig.LOB_ENABLED, Boolean.TRUE);
    }

    private static String topicName(String tableName) {
        return String.format("%s.DEBEZIUM.%s", TestHelper.SERVER_NAME, tableName);
    }

    private static Struct getBefore(SourceRecord record) {
        return ((Struct) record.value()).getStruct(BEFORE);
    }

    private static Struct getAfter(SourceRecord record) {
        return ((Struct) record.value()).getStruct(AFTER);
    }

    private static void assertUnavailableValue(Object value) {
        assertThat(value).isEqualTo("__debezium_unavailable_value");
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * Integration tests for JSON data type support.
 *
 * @author Artem Shubovych
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "LogMiner specific tests")
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 21, reason = "JSON columns are only supported in Oracle 21+")
public class OracleJSONDataTypeIT extends AbstractConnectorTest {

    private static final String JSON_DATA_RAW = Files.readResourceAsString("data/test_lob_data.json");

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnection connection;

    @Before
    public void before() {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-6993")
    public void shouldSnapshotTableWithJsonColumnType() throws Exception {
        stopConnector();
        TestHelper.dropTable(connection, "dbz6993");

        connection.execute("CREATE TABLE DBZ6993 (ID numeric(9,0), DATA JSON, primary key(ID))");
        TestHelper.streamTable(connection, "dbz6993");

        connection.prepareQuery("insert into dbz6993 values (1, ?)", ps -> ps.setString(1, JSON_DATA_RAW), null);
        connection.commit();

        Configuration config = getDefaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6993")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        SourceRecord record = topicRecords.get(0);
        VerifyRecord.isValidRead(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);

        // JSON data can not be correctly and consistently compared as strings
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(JSON_DATA_RAW));
    }

    @Test
    @FixFor("DBZ-6993")
    public void shouldStreamTableWithJsonColumnType() throws Exception {
        stopConnector();
        TestHelper.dropTable(connection, "dbz6993");

        connection.execute("CREATE TABLE DBZ6993 (ID numeric(9,0), DATA JSON, primary key(ID))");
        TestHelper.streamTable(connection, "dbz6993");

        Configuration config = getDefaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6993")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        final List<Person> jsonRecords = Arrays.asList(objectMapper.readValue(JSON_DATA_RAW, Person[].class));
        final List<Person> page1 = jsonRecords.stream().limit(20).collect(Collectors.toList());
        final String page1Raw = objectMapper.writeValueAsString(page1);

        connection.prepareQuery("insert into dbz6993 values (1, ?)", ps -> ps.setString(1, page1Raw), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        SourceRecord record = topicRecords.get(0);
        VerifyRecord.isValidInsert(record, "ID", 1);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page1Raw));

        final List<Person> page2 = jsonRecords.stream().skip(20).limit(20).collect(Collectors.toList());
        final String page2Raw = objectMapper.writeValueAsString(page2);

        connection.prepareQuery("UPDATE dbz6993 SET data = ? WHERE id = 1", ps -> ps.setString(1, page2Raw), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        record = topicRecords.get(0);
        VerifyRecord.isValidUpdate(record, "ID", 1);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page2Raw));

        connection.execute("DELETE FROM dbz6993 WHERE id = 1");

        records = consumeRecordsByTopic(1);
        topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        record = topicRecords.get(0);
        VerifyRecord.isValidDelete(record, "ID", 1);

        after = before(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page2Raw));

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-6993")
    public void shouldStreamTableWithJsonTypeColumnAndOtherNonJsonColumns() throws Exception {
        stopConnector();

        // This tests makes sure there are no special requirements when a table is keyless
        TestHelper.dropTable(connection, "dbz6993");

        // Explicitly no key.
        connection.execute("CREATE TABLE DBZ6993 (ID numeric(9,0), DATA JSON, DATA2 varchar2(50))");
        TestHelper.streamTable(connection, "dbz6993");

        Configuration config = getDefaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6993")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        final List<Person> jsonRecords = Arrays.asList(objectMapper.readValue(JSON_DATA_RAW, Person[].class));
        final List<Person> page1 = jsonRecords.stream().limit(20).collect(Collectors.toList());
        final String page1Raw = objectMapper.writeValueAsString(page1);

        connection.prepareQuery("insert into dbz6993 values (1, ?, 'Acme')", ps -> ps.setString(1, page1Raw), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        SourceRecord record = topicRecords.get(0);
        VerifyRecord.isValidInsert(record, false);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(JSON_DATA_RAW));
        assertThat(after.get("DATA2")).isEqualTo("Acme");

        // Update only RAW
        final List<Person> page2 = jsonRecords.stream().skip(20).limit(20).collect(Collectors.toList());
        final String page2Raw = objectMapper.writeValueAsString(page2);

        connection.prepareQuery("UPDATE dbz6993 SET data = ? WHERE id=1", ps -> ps.setString(1, page2Raw), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        record = topicRecords.get(0);
        VerifyRecord.isValidUpdate(record, false);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page2Raw));
        assertThat(after.get("DATA2")).isEqualTo("Acme");

        // Update RAW and non-RAW
        connection.prepareQuery("UPDATE dbz6993 SET data = ?, DATA2 = 'Data' WHERE id=1", ps -> ps.setString(1, page1Raw), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        record = topicRecords.get(0);
        VerifyRecord.isValidUpdate(record, false);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page1Raw));
        assertThat(after.get("DATA2")).isEqualTo("Data");

        // Update only non-RAW
        connection.execute("UPDATE dbz6993 SET DATA2 = 'Acme' WHERE id=1");

        records = consumeRecordsByTopic(1);
        topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        record = topicRecords.get(0);
        VerifyRecord.isValidUpdate(record, false);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page1Raw));
        assertThat(after.get("DATA2")).isEqualTo("Acme");

        connection.execute("DELETE FROM dbz6993 WHERE id = 1");

        records = consumeRecordsByTopic(1);
        topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        record = topicRecords.get(0);
        VerifyRecord.isValidDelete(record, false);

        after = before(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page1Raw));
        assertThat(after.get("DATA2")).isEqualTo("Acme");

        assertThat(after(record)).isNull();
    }

    @Test
    @FixFor("DBZ-6993")
    public void shouldStreamTableWithNoPrimaryKeyWithJsonTypeColumn() throws Exception {
        stopConnector();

        // This tests makes sure there are no special requirements when a table is keyless
        TestHelper.dropTable(connection, "dbz6993");

        // Explicitly no key.
        connection.execute("CREATE TABLE DBZ6993 (ID numeric(9,0), DATA JSON)");
        TestHelper.streamTable(connection, "dbz6993");

        Configuration config = getDefaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6993")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        final List<Person> jsonRecords = Arrays.asList(objectMapper.readValue(JSON_DATA_RAW, Person[].class));
        final List<Person> page1 = jsonRecords.stream().limit(20).collect(Collectors.toList());
        final String page1Raw = objectMapper.writeValueAsString(page1);

        connection.prepareQuery("insert into dbz6993 values (1,?)", ps -> ps.setString(1, page1Raw), null);
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        SourceRecord record = topicRecords.get(0);
        VerifyRecord.isValidInsert(record, false);

        Struct after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page1Raw));

        final List<Person> page2 = jsonRecords.stream().skip(20).limit(20).collect(Collectors.toList());
        final String page2Raw = objectMapper.writeValueAsString(page2);

        connection.prepareQuery("UPDATE dbz6993 SET data = ? WHERE id=1", ps -> ps.setObject(1, page2Raw), null);
        connection.commit();

        records = consumeRecordsByTopic(1);
        topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        record = topicRecords.get(0);
        VerifyRecord.isValidUpdate(record, false);

        after = after(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page2Raw));

        connection.execute("DELETE FROM dbz6993 WHERE id = 1");

        records = consumeRecordsByTopic(1);
        topicRecords = records.recordsForTopic(topicName("DBZ6993"));
        assertThat(topicRecords).hasSize(1);

        record = topicRecords.get(0);
        VerifyRecord.isValidDelete(record, false);

        after = before(record);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(objectMapper.readTree((String) after.get("DATA"))).isEqualTo(objectMapper.readTree(page2Raw));

        assertThat(after(record)).isNull();
    }

    private Configuration.Builder getDefaultConfig() {
        return TestHelper.defaultConfig();
    }

    private static String topicName(String tableName) {
        return TestHelper.SERVER_NAME + ".DEBEZIUM." + tableName;
    }

    private static Struct before(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.BEFORE);
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    @JsonAutoDetect
    public static class Person {
        public String _id;
        public Integer index;
        public String guid;
        public Boolean isActive;
        public String balance;
        public String picture;
        public Integer age;
        public String eyeColor;

        public Name name;
        public String company;
        public String email;
        public String phone;
        public String address;
        public String about;
        public String registered;
        public String latitude;
        public String longitude;

        public List<String> tags;

        public List<Integer> range;

        public List<Friend> friends;

        public String greeting;
        public String favoriteFruit;

        @JsonAutoDetect
        public static class Name {
            public String first;
            public String last;
        }

        @JsonAutoDetect
        public static class Friend {
            public Integer id;
            public String name;
        }
    }

}

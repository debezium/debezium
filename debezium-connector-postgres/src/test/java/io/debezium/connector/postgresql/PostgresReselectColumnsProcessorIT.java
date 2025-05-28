/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.HStoreHandlingMode.JSON;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.HStoreHandlingMode.MAP;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.processors.AbstractReselectProcessorTest;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;

/**
 * Postgres' integration tests for {@link ReselectColumnsPostProcessor}.
 *
 * @author Chris Cranford
 */
public class PostgresReselectColumnsProcessorIT extends AbstractReselectProcessorTest<PostgresConnector> {

    public static final String CREATE_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "CREATE SCHEMA s1; ";

    private PostgresConnection connection;

    @Before
    public void beforeEach() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute(CREATE_STMT);
        connection = TestHelper.create();
        super.beforeEach();
    }

    @After
    public void afterEach() throws Exception {
        super.afterEach();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Override
    protected Class<PostgresConnector> getConnectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz4321")
                .with(PostgresConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("reselector.type", ReselectColumnsPostProcessor.class.getName());
    }

    @Override
    protected String topicName() {
        return "test_server.s1.dbz4321";
    }

    @Override
    protected String tableName() {
        return "s1.dbz4321";
    }

    @Override
    protected String reselectColumnsList() {
        return "s1.dbz4321:data";
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz4321 (id int primary key, data varchar(50), data2 int);");
        TestHelper.execute("ALTER TABLE s1.dbz4321 REPLICA IDENTITY FULL;");
    }

    @Override
    protected void dropTable() throws Exception {
    }

    @Override
    protected String getInsertWithValue() {
        return "INSERT INTO s1.dbz4321 (id,data,data2) values (1,'one',1);";
    }

    @Override
    protected String getInsertWithNullValue() {
        return "INSERT INTO s1.dbz4321 (id,data,data2) values (1,null,1);";
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }

    @Test
    @FixFor("DBZ-8168")
    public void testToastColumnReselectedWhenJsonbValueIsUnavailable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8168_toast (id int primary key, data jsonb, data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz8168_toast")
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        final String json = "{\"key\": \"" + RandomStringUtils.randomAlphabetic(10000) + "\"}";

        TestHelper.execute("INSERT INTO s1.dbz8168_toast (id,data,data2) values (1,'" + json + "',1);",
                "UPDATE s1.dbz8168_toast SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz8168_toast");

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(json);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(json);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz8168_toast", "data");
    }

    @Test
    @FixFor("DBZ-4321")
    public void testToastColumnReselectedWhenValueIsUnavailable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz4321_toast (id int primary key, data text, data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz4321_toast")
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        final String text = RandomStringUtils.randomAlphabetic(10000);

        TestHelper.execute("INSERT INTO s1.dbz4321_toast (id,data,data2) values (1,'" + text + "',1);",
                "UPDATE s1.dbz4321_toast SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz4321_toast");

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(text);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(text);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz4321_toast", "data");
    }

    @Test
    @FixFor("DBZ-7596")
    public void testToastColumnHstoreAsMapReselectedWhenValueIsUnavailable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz7596_toast (id int primary key, data hstore, data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        final String data = RandomStringUtils.randomAlphanumeric(8192);

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz7596_toast")
                .with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, MAP.getValue())
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        TestHelper.execute(
                "INSERT INTO s1.dbz7596_toast (id,data,data2) values (1,'\"key\"=>\"" + data + "\"', 1);",
                "UPDATE s1.dbz7596_toast SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz7596_toast");

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(Map.of("key", data));
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(Map.of("key", data));
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz7596_toast", "data");
    }

    @Test
    @FixFor("DBZ-7596")
    public void testToastColumnHstoreAsJsonReselectedWhenValueIsUnavailable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz7596_toast (id int primary key, data hstore, data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        final String data = RandomStringUtils.randomAlphanumeric(8192);
        final String expectedData = "{\"key\":\"" + data + "\"}";

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz7596_toast")
                .with(PostgresConnectorConfig.HSTORE_HANDLING_MODE, JSON.getValue())
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        TestHelper.execute(
                "INSERT INTO s1.dbz7596_toast (id,data,data2) values (1,'\"key\"=>\"" + data + "\"', 1);",
                "UPDATE s1.dbz7596_toast SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz7596_toast");

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(expectedData);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(expectedData);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz7596_toast", "data");
    }

    @Test
    @FixFor("DBZ-7596")
    public void testToastColumnTextArrayReselectedWhenValueIsUnavailable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz7596_toast (id int primary key, data text[], data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        final List<String> textValues = new ArrayList<>();
        textValues.add(RandomStringUtils.randomAlphanumeric(8192));
        textValues.add(RandomStringUtils.randomAlphanumeric(8192));

        final String data = textValues.stream().map(v -> "\"" + v + "\"").collect(Collectors.joining(", "));

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz7596_toast")
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, "true")
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        TestHelper.execute(
                "INSERT INTO s1.dbz7596_toast (id,data,data2) values (1,'{" + data + "}', 1);",
                "UPDATE s1.dbz7596_toast SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz7596_toast");

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(textValues);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(textValues);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz7596_toast", "data");
    }

    @Test
    @FixFor("DBZ-8212")
    public void testToastColumnIntArrayReselectedWhenValueIsUnavailable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8212_toast (id int primary key, data bigint[], data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        final var arraySize = 8192;
        final List<Long> longValues = new ArrayList<>(arraySize);
        for (int i = 0; i < arraySize; i++) {
            longValues.add(RandomUtils.nextLong());
        }

        final String data = longValues.stream().map(x -> x.toString()).collect(Collectors.joining(", "));

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz8212_toast")
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, "true")
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        TestHelper.execute(
                "INSERT INTO s1.dbz8212_toast (id,data,data2) values (1,'{" + data + "}', 1);",
                "UPDATE s1.dbz8212_toast SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz8212_toast");

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(longValues);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(longValues);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz8212_toast", "data");
    }

    @Test
    @FixFor("DBZ-8277")
    public void testToastColumnReselectedWhenPrimaryKeyIsUUIDType() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8277_toast (id uuid primary key, data text[], data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        final List<String> textValues = new ArrayList<>();
        textValues.add(RandomStringUtils.randomAlphanumeric(8192));
        textValues.add(RandomStringUtils.randomAlphanumeric(8192));

        final String data = textValues.stream().map(v -> "\"" + v + "\"").collect(Collectors.joining(", "));

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz8277_toast")
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, "true")
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        TestHelper.execute(
                "INSERT INTO s1.dbz8277_toast (id,data,data2) values ('b1164c91-b574-495c-9f17-c4899d88404c'::uuid,'{" + data + "}', 1);",
                "UPDATE s1.dbz8277_toast SET data2 = 2 where id = 'b1164c91-b574-495c-9f17-c4899d88404c';");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz8277_toast");

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("id")).isEqualTo("b1164c91-b574-495c-9f17-c4899d88404c");
        assertThat(after.get("data")).isEqualTo(textValues);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("id")).isEqualTo("b1164c91-b574-495c-9f17-c4899d88404c");
        assertThat(after.get("data")).isEqualTo(textValues);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz8277_toast", "data");
    }

    @Test
    @FixFor("DBZ-9086")
    public void testToastColumnReselectedWhenPrimaryKeyIsSerialType() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz9086_toast (id serial primary key, data text, data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz9086_toast")
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        final String text = RandomStringUtils.randomAlphabetic(10000);

        TestHelper.execute("INSERT INTO s1.dbz9086_toast (id,data,data2) values (1,'" + text + "',1);",
                "UPDATE s1.dbz9086_toast SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz9086_toast");

        // Check insert
        SourceRecord record = tableRecords.get(0);
        Struct after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidInsert(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(text);
        assertThat(after.get("data2")).isEqualTo(1);

        // Check update
        record = tableRecords.get(1);
        after = ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
        VerifyRecord.isValidUpdate(record, "id", 1);
        assertThat(after.get("id")).isEqualTo(1);
        assertThat(after.get("data")).isEqualTo(text);
        assertThat(after.get("data2")).isEqualTo(2);

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz9086_toast", "data");
    }
}

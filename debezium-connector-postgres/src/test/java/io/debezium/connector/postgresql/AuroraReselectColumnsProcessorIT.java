/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.processors.AuroraReselectColumnsPostProcessor;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.processors.AbstractReselectProcessorTest;

/**
 * Aurora Postgres' integration tests for {@link io.debezium.connector.postgresql.processors.AuroraReselectColumnsPostProcessor}.
 *
 * @author Gaurav Miglani
 */
public class AuroraReselectColumnsProcessorIT extends AbstractReselectProcessorTest<PostgresConnector> {

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
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz8288")
                .with(PostgresConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("reselector.type", AuroraReselectColumnsPostProcessor.class.getName())
                .with("reselector.reader.host", "localhost") // keeping same supplement host as reader
                .with("reselector.reader.port", 5432);
    }

    @Override
    protected String topicName() {
        return "test_server.s1.dbz8288";
    }

    @Override
    protected String tableName() {
        return "s1.dbz8288";
    }

    @Override
    protected String reselectColumnsList() {
        return "s1.dbz8288:data";
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8288 (id int primary key, data text, data2 int);");
        TestHelper.execute("ALTER TABLE s1.dbz8288 REPLICA IDENTITY FULL;");
    }

    @Override
    protected void dropTable() throws Exception {
    }

    @Override
    protected String getInsertWithValue() {
        return "INSERT INTO s1.dbz8288 (id,data,data2) values (1,'one',1);";
    }

    @Override
    protected String getInsertWithNullValue() {
        return "INSERT INTO s1.dbz8288 (id,data,data2) values (1,null,1);";
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }

    @Test
    public void testToastColumnReselectedWithAuroraReader() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8288_toast (id int primary key, data text, data2 int);");

        final LogInterceptor logInterceptor = getReselectLogInterceptor();

        Configuration config = getConfigurationBuilder()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz8288_toast")
                .build();

        start(PostgresConnector.class, config);
        waitForStreamingStarted();

        final String text = RandomStringUtils.randomAlphabetic(10000);

        TestHelper.execute("INSERT INTO s1.dbz8288_toast (id,data,data2) values (1,'" + text + "',1);",
                "UPDATE s1.dbz8288_toast SET data2 = 2 where id = 1;");

        final SourceRecords sourceRecords = consumeRecordsByTopic(2);
        final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("test_server.s1.dbz8288_toast");

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

        assertColumnReselectedForUnavailableValue(logInterceptor, "s1.dbz8288_toast", "data");
    }
}

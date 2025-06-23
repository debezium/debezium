/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipTestDependingOnStrategyRule;
import io.debezium.connector.oracle.junit.SkipWhenLogMiningStrategyIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.processors.AbstractReselectProcessorTest;
import io.debezium.processors.reselect.ReselectColumnsPostProcessor;

/**
 * Oracle's integration tests for {@link ReselectColumnsPostProcessor}.
 *
 * @author Chris Cranford
 */
public class OracleReselectColumnsProcessorIT extends AbstractReselectProcessorTest<OracleConnector> {

    @Rule
    public final TestRule skipStrategyRule = new SkipTestDependingOnStrategyRule();

    private OracleConnection connection;

    @Before
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        super.beforeEach();
    }

    @After
    public void afterEach() throws Exception {
        super.afterEach();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    protected Class<OracleConnector> getConnectorClass() {
        return OracleConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4321")
                .with(OracleConnectorConfig.CUSTOM_POST_PROCESSORS, "reselector")
                .with("post.processors.reselector.type", ReselectColumnsPostProcessor.class.getName());
    }

    @Override
    protected String topicName() {
        return TestHelper.SERVER_NAME + ".DEBEZIUM.DBZ4321";
    }

    @Override
    protected String tableName() {
        return "DEBEZIUM.DBZ4321";
    }

    @Override
    protected String reselectColumnsList() {
        return "DEBEZIUM.DBZ4321:DATA";
    }

    @Override
    protected void createTable() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data varchar2(50), data2 numeric(9,0))");
        TestHelper.streamTable(connection, "dbz4321");
    }

    @Override
    protected void dropTable() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
    }

    @Override
    protected String getInsertWithValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,'one',1)";
    }

    @Override
    protected String getInsertWithNullValue() {
        return "INSERT INTO dbz4321 (id,data,data2) values (1,null,1)";
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    @Override
    protected String fieldName(String fieldName) {
        return fieldName.toUpperCase();
    }

    @Test
    @FixFor("DBZ-7729")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void testColumnReselectionUsesPrimaryKeyColumnAndValuesDespiteMessageKeyColumnConfigs() throws Exception {
        TestHelper.dropTable(connection, "dbz7729");
        try {
            final LogInterceptor logInterceptor = getReselectLogInterceptor();

            connection.execute("CREATE TABLE dbz7729 (id numeric(9,0) primary key, data clob, data2 numeric(9,0), data3 varchar2(25))");
            TestHelper.streamTable(connection, "dbz7729");

            Configuration config = getConfigurationBuilder()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ7729")
                    .with(OracleConnectorConfig.MSG_KEY_COLUMNS, "(.*).DEBEZIUM.DBZ7729:DATA3")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with("post.processors.reselector.reselect.columns.include.list", "DEBEZIUM.DBZ7729:DATA")
                    .build();

            start(getConnectorClass(), config);
            assertConnectorIsRunning();

            waitForStreamingStarted();

            // Insert will always include the data
            final String clobData = RandomStringUtils.randomAlphabetic(10000);
            final Clob clob = connection.connection().createClob();
            clob.setString(1, clobData);
            connection.prepareQuery("INSERT INTO dbz7729 (id,data,data2,data3) values (1,?,1,'A')", ps -> ps.setClob(1, clob), null);
            connection.commit();

            // Update row without changing clob
            connection.execute("UPDATE dbz7729 set data2=10 where id = 1");

            final SourceRecords sourceRecords = consumeRecordsByTopic(2);

            final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ7729");
            assertThat(tableRecords).hasSize(2);

            SourceRecord update = tableRecords.get(1);
            VerifyRecord.isValidUpdate(update, true);

            Struct key = ((Struct) update.key());
            assertThat(key.schema().fields()).hasSize(1);
            assertThat(key.get("DATA3")).isEqualTo("A");

            Struct after = ((Struct) update.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(clobData);
            assertThat(after.get("DATA2")).isEqualTo(10);
            assertThat(after.get("DATA3")).isEqualTo("A");

            assertColumnReselectedForUnavailableValue(logInterceptor, TestHelper.getDatabaseName() + ".DEBEZIUM.DBZ7729", "DATA");
        }
        finally {
            TestHelper.dropTable(connection, "dbz7729");
        }
    }

    @Test
    @FixFor("DBZ-4321")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void testClobReselectedWhenValueIsUnavailable() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            final LogInterceptor logInterceptor = getReselectLogInterceptor();

            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data clob, data2 numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = getConfigurationBuilder()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with("post.processors.reselector.reselect.columns.include.list", reselectColumnsList())
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingStarted();

            // Insert will always include the data
            final String clobData = RandomStringUtils.randomAlphabetic(10000);
            final Clob clob = connection.connection().createClob();
            clob.setString(1, clobData);
            connection.prepareQuery("INSERT INTO dbz4321 (id,data,data2) values (1,?,1)", ps -> ps.setClob(1, clob), null);
            connection.commit();

            // Update row without changing clob
            connection.execute("UPDATE dbz4321 set data2=10 where id = 1");

            final SourceRecords sourceRecords = consumeRecordsByTopic(2);

            final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(tableRecords).hasSize(2);

            SourceRecord update = tableRecords.get(1);
            VerifyRecord.isValidUpdate(update, "ID", 1);

            Struct after = ((Struct) update.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(clobData);
            assertThat(after.get("DATA2")).isEqualTo(10);

            assertColumnReselectedForUnavailableValue(logInterceptor, TestHelper.getDatabaseName() + ".DEBEZIUM.DBZ4321", "DATA");
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }

    @Test
    @FixFor("DBZ-4321")
    @SkipWhenLogMiningStrategyIs(value = SkipWhenLogMiningStrategyIs.Strategy.HYBRID, reason = "Cannot use lob.enabled with Hybrid")
    public void testBlobReselectedWhenValueIsUnavailable() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            final LogInterceptor logInterceptor = getReselectLogInterceptor();

            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data blob, data2 numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = getConfigurationBuilder()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with("post.processors.reselector.reselect.columns.include.list", reselectColumnsList())
                    .build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingStarted();

            // Insert will always include the data
            final byte[] blobData = RandomStringUtils.random(10000).getBytes(StandardCharsets.UTF_8);
            final Blob blob = connection.connection().createBlob();
            blob.setBytes(1, blobData);
            connection.prepareQuery("INSERT INTO dbz4321 (id,data,data2) values (1,?,1)", ps -> ps.setBlob(1, blob), null);
            connection.commit();

            // Update row without changing clob
            connection.execute("UPDATE dbz4321 set data2=10 where id = 1");

            final SourceRecords sourceRecords = consumeRecordsByTopic(2);

            final List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(tableRecords).hasSize(2);

            SourceRecord update = tableRecords.get(1);
            VerifyRecord.isValidUpdate(update, "ID", 1);

            Struct after = ((Struct) update.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(blobData));
            assertThat(after.get("DATA2")).isEqualTo(10);

            assertColumnReselectedForUnavailableValue(logInterceptor, TestHelper.getDatabaseName() + ".DEBEZIUM.DBZ4321", "DATA");
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }

    @Test
    @FixFor("DBZ-8493")
    @Ignore("This requires running ALTER SYSTEM SET UNDO_RETENTION=60 within the PDB, which we do not want to automate")
    public void testShouldNotThrowErrorUsingFallbackQuery() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            final LogInterceptor logInterceptor = getReselectLogInterceptor();

            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data clob, data2 numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = getConfigurationBuilder()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with("post.processors.reselector.reselect.columns.include.list", reselectColumnsList())
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingStarted();

            // Insert will always include the data
            final String clobData = RandomStringUtils.randomAlphanumeric(10000);
            final Clob clob = connection.connection().createClob();
            clob.setString(1, clobData);
            connection.prepareQuery("INSERT INTO dbz4321 (id,data,data2) values (1,?,1)", ps -> ps.setClob(1, clob), null);
            connection.commit();

            SourceRecords sourceRecords = consumeRecordsByTopic(1);

            List<SourceRecord> tableRecords = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(tableRecords).hasSize(1);

            SourceRecord insert = tableRecords.get(0);
            VerifyRecord.isValidInsert(insert, "ID", 1);

            Struct after = ((Struct) insert.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(clobData);
            assertThat(after.get("DATA2")).isEqualTo(1);

            // Temporary stop connector
            stopConnector();

            // Perform update while connector is down
            connection.execute("UPDATE dbz4321 set data2 = 10 where id = 1");

            // We need to wait until the undo retention period before we continue
            // This will pause the test until the time has reached
            final long undoRetentionSeconds = TestHelper.getUndoRetentionSeconds();
            System.out.println("UNDO_RETENTION is set to " + undoRetentionSeconds + " seconds, waiting until it expires.");

            final AtomicLong value = new AtomicLong();
            Awaitility.await().atMost(undoRetentionSeconds * 3, TimeUnit.SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        System.out.println("Total time waited: " + value.get() + " seconds");
                        return value.addAndGet(1) >= undoRetentionSeconds * 2;
                    });

            // Now that undo retention has expired, restart the connector
            // We should get the CLOB column reselected with data
            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingStarted();

            sourceRecords = consumeRecordsByTopic(1);

            tableRecords = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(tableRecords).hasSize(1);

            SourceRecord update = tableRecords.get(0);
            VerifyRecord.isValidUpdate(update, "ID", 1);

            after = ((Struct) update.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("ID")).isEqualTo(1);
            assertThat(after.get("DATA")).isEqualTo(clobData);
            assertThat(after.get("DATA2")).isEqualTo(10);
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }
}

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.transforms.reselect.ReselectColumns;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;

/**
 * @author Chris Cranford
 */
public class OracleReselectColumnsIT extends AbstractConnectorTest {

    private OracleConnection connection;

    @Before
    public void beforeEach() {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void afterEach() throws Exception {
        if (connection != null && connection.isConnected()) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldNotReselectClobColumnWhenPopulated() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data clob, val numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4321")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            LogInterceptor interceptor = new LogInterceptor(ReselectColumns.class);
            interceptor.setLoggerLevel(ReselectColumns.class, Level.DEBUG);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = RandomStringUtils.randomAlphabetic(5000);
            final Clob clob = connection.connection().createClob();
            clob.setString(1, data);
            connection.prepareQuery("INSERT INTO dbz4321 values (1,?,1)", ps -> ps.setClob(1, clob), null);
            connection.commit();

            SourceRecords sourceRecords = consumeRecordsByTopic(1);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(records).hasSize(1);

            // Should automatically detect unavailable value placeholders
            try (ReselectColumns<SourceRecord> transform = createTransform(config.asMap())) {
                // Verify Insert
                SourceRecord record = records.get(0);
                VerifyRecord.isValidInsert(record, "ID", 1);

                SourceRecord transformed = transform.apply(record);
                Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isEqualTo(data);
                assertThat(after.get("VAL")).isEqualTo(1);
            }

            // Verify no re-selection happened
            assertThat(interceptor.containsMessage("No columns require reselect.")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldReselectClobWhenEmittedAsUnavailableValuePlaceholder() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data clob, val numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4321")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = RandomStringUtils.randomAlphabetic(5000);
            final Clob clob = connection.connection().createClob();
            clob.setString(1, data);
            connection.prepareQuery("INSERT INTO dbz4321 values (1,?, 1)", ps -> ps.setClob(1, clob), null);
            connection.commit();

            // Update non-CLOB
            connection.execute("UPDATE dbz4321 set val=2 where id = 1");

            SourceRecords sourceRecords = consumeRecordsByTopic(2);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(records).hasSize(2);

            // Should automatically detect unavailable value placeholders
            try (ReselectColumns<SourceRecord> transform = createTransform(config.asMap())) {
                // Verify the insert
                SourceRecord record = records.get(0);
                VerifyRecord.isValidInsert(record, "ID", 1);

                SourceRecord transformed = transform.apply(record);
                Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isEqualTo(data);
                assertThat(after.get("VAL")).isEqualTo(1);

                // Verify the update
                record = records.get(1);
                VerifyRecord.isValidUpdate(record, "ID", 1);

                transformed = transform.apply(record);
                after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isEqualTo(data);
                assertThat(after.get("VAL")).isEqualTo(2);
            }
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldNotReselectBlobColumnWhenPopulated() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data blob, val numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4321")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            LogInterceptor interceptor = new LogInterceptor(ReselectColumns.class);
            interceptor.setLoggerLevel(ReselectColumns.class, Level.DEBUG);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = RandomStringUtils.random(5000);
            final Blob blob = connection.connection().createBlob();
            blob.setBytes(1, data.getBytes(StandardCharsets.UTF_8));
            connection.prepareQuery("INSERT INTO dbz4321 values (1,?,1)", ps -> ps.setBlob(1, blob), null);
            connection.commit();

            SourceRecords sourceRecords = consumeRecordsByTopic(1);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(records).hasSize(1);

            // Should automatically detect unavailable value placeholders
            try (ReselectColumns<SourceRecord> transform = createTransform(config.asMap())) {
                // Verify Insert
                SourceRecord record = records.get(0);
                VerifyRecord.isValidInsert(record, "ID", 1);

                SourceRecord transformed = transform.apply(record);
                Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
                assertThat(after.get("VAL")).isEqualTo(1);
            }

            // Verify no re-selection happened
            assertThat(interceptor.containsMessage("No columns require reselect.")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldReselectBlobWhenEmittedAsUnavailableValuePlaceholder() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data blob, val numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4321")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = RandomStringUtils.random(5000);
            final Blob blob = connection.connection().createBlob();
            blob.setBytes(1, data.getBytes(StandardCharsets.UTF_8));
            connection.prepareQuery("INSERT INTO dbz4321 values (1,?, 1)", ps -> ps.setBlob(1, blob), null);
            connection.commit();

            // Update non-CLOB
            connection.execute("UPDATE dbz4321 set val=2 where id = 1");

            SourceRecords sourceRecords = consumeRecordsByTopic(2);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(records).hasSize(2);

            // Should automatically detect unavailable value placeholders
            try (ReselectColumns<SourceRecord> transform = createTransform(config.asMap())) {
                // Verify the insert
                SourceRecord record = records.get(0);
                VerifyRecord.isValidInsert(record, "ID", 1);

                SourceRecord transformed = transform.apply(record);
                Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
                assertThat(after.get("VAL")).isEqualTo(1);

                // Verify the update
                record = records.get(1);
                VerifyRecord.isValidUpdate(record, "ID", 1);

                transformed = transform.apply(record);
                after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isEqualTo(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)));
                assertThat(after.get("VAL")).isEqualTo(2);
            }
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldNotReselectSpecifiedColumnWhenNullValue() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data varchar2(4000), val numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4321")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            LogInterceptor interceptor = new LogInterceptor(ReselectColumns.class);
            interceptor.setLoggerLevel(ReselectColumns.class, Level.DEBUG);

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = RandomStringUtils.randomAlphabetic(4000);
            connection.prepareQuery("INSERT INTO dbz4321 values (1,?,1)", ps -> ps.setString(1, data), null);
            connection.commit();

            SourceRecords sourceRecords = consumeRecordsByTopic(1);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(records).hasSize(1);

            Map<String, String> properties = config.asMap();
            properties.put("column.list", "DATA");

            try (ReselectColumns<SourceRecord> transform = createTransform(properties)) {
                // Verify Insert
                SourceRecord record = records.get(0);
                VerifyRecord.isValidInsert(record, "ID", 1);

                SourceRecord transformed = transform.apply(record);
                Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isEqualTo(data);
                assertThat(after.get("VAL")).isEqualTo(1);
            }

            // Verify no re-selection happened
            assertThat(interceptor.containsMessage("No columns require reselect.")).isTrue();
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldReselectSpecifiedColumnWhenNullValue() throws Exception {
        TestHelper.dropTable(connection, "dbz4321");
        try {
            connection.execute("CREATE TABLE dbz4321 (id numeric(9,0) primary key, data varchar2(4000), val numeric(9,0))");
            TestHelper.streamTable(connection, "dbz4321");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ4321")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = RandomStringUtils.randomAlphabetic(4000);
            connection.prepareQuery("INSERT INTO dbz4321 values (1,?, 1)", ps -> ps.setString(1, data), null);
            connection.commit();

            // Update non-CLOB
            connection.execute("UPDATE dbz4321 set val=2, data = null where id = 1");

            SourceRecords sourceRecords = consumeRecordsByTopic(2);
            List<SourceRecord> records = sourceRecords.recordsForTopic("server1.DEBEZIUM.DBZ4321");
            assertThat(records).hasSize(2);

            Map<String, String> properties = config.asMap();
            properties.put("column.list", "DATA");

            LogInterceptor interceptor = new LogInterceptor(ReselectColumns.class);
            interceptor.setLoggerLevel(ReselectColumns.class, Level.DEBUG);

            try (ReselectColumns<SourceRecord> transform = createTransform(properties)) {
                // Verify the insert
                SourceRecord record = records.get(0);
                VerifyRecord.isValidInsert(record, "ID", 1);

                SourceRecord transformed = transform.apply(record);
                Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isEqualTo(data);
                assertThat(after.get("VAL")).isEqualTo(1);

                // Verify no re-selection happened
                assertThat(interceptor.containsMessage("No columns require reselect.")).isTrue();
                interceptor.clear();

                // Verify the update
                record = records.get(1);
                VerifyRecord.isValidUpdate(record, "ID", 1);

                transformed = transform.apply(record);
                after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
                assertThat(after.get("ID")).isEqualTo(1);
                assertThat(after.get("DATA")).isNull();
                assertThat(after.get("VAL")).isEqualTo(2);

                // Null will always trigger a re-select
                assertThat(interceptor.containsMessage("No columns require reselect.")).isFalse();
            }
        }
        finally {
            TestHelper.dropTable(connection, "dbz4321");
        }
    }

    private ReselectColumns<SourceRecord> createTransform(Map<String, String> properties) {
        properties.put("connection.user", TestHelper.getConnectorUserName());
        properties.put("connection.password", TestHelper.CONNECTOR_USER_PASS);
        properties.put("connection.url", TestHelper.getOracleConnectionUrlDescriptor());

        final ReselectColumns<SourceRecord> transform = new ReselectColumns<>();
        transform.configure(properties);
        return transform;
    }

}

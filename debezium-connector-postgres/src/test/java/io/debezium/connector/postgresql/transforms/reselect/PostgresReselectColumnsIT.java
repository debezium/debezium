/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.reselect;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.transforms.reselect.ReselectColumns;

import ch.qos.logback.classic.Level;

/**
 * @author Chris Cranford
 */
public class PostgresReselectColumnsIT extends AbstractConnectorTest {

    @Before
    public void beforeEach() throws SQLException {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE dbz4321 (id int primary key, data text, val int)");
    }

    @After
    public void afterEach() throws Exception {
        TestHelper.dropAllSchemas();
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldNotReselectToastColumnWhenPopulated() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public\\.dbz4321")
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        LogInterceptor interceptor = new LogInterceptor(ReselectColumns.class);
        interceptor.setLoggerLevel(ReselectColumns.class, Level.DEBUG);

        final String data = RandomStringUtils.randomAlphabetic(4000);
        TestHelper.execute("INSERT INTO dbz4321 values (1,'" + data + "', 1)");

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> records = sourceRecords.recordsForTopic("test_server.public.dbz4321");
        assertThat(records).hasSize(1);

        try (ReselectColumns<SourceRecord> transform = createTransform(config.asMap())) {
            // Verify Insert
            SourceRecord record = records.get(0);
            VerifyRecord.isValidInsert(record, "id", 1);

            SourceRecord transformed = transform.apply(record);
            Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("id")).isEqualTo(1);
            assertThat(after.get("data")).isEqualTo(data);
            assertThat(after.get("val")).isEqualTo(1);
        }

        // Verify no re-selection happened
        assertThat(interceptor.containsMessage("No columns require reselect.")).isTrue();
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldReselectToastColumnWhenEmittedAsUnavailableValuePlaceholder() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public\\.dbz4321")
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        final String data = RandomStringUtils.randomAlphabetic(4000);
        TestHelper.execute("INSERT INTO dbz4321 values (1,'" + data + "', 1)");
        // Update non-Toast
        TestHelper.execute("UPDATE dbz4321 set val=2 where id = 1");

        SourceRecords sourceRecords = consumeRecordsByTopic(2);
        sourceRecords.allRecordsInOrder().forEach(System.out::println);
        List<SourceRecord> records = sourceRecords.recordsForTopic("test_server.public.dbz4321");
        assertThat(records).hasSize(2);

        LogInterceptor interceptor = new LogInterceptor(ReselectColumns.class);
        interceptor.setLoggerLevel(ReselectColumns.class, Level.DEBUG);

        try (ReselectColumns<SourceRecord> transform = createTransform(config.asMap())) {
            // Verify the insert
            SourceRecord record = records.get(0);
            VerifyRecord.isValidInsert(record, "id", 1);

            SourceRecord transformed = transform.apply(record);
            Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("id")).isEqualTo(1);
            assertThat(after.get("data")).isEqualTo(data);
            assertThat(after.get("val")).isEqualTo(1);

            // Verify no re-selection happened
            assertThat(interceptor.containsMessage("No columns require reselect.")).isTrue();
            interceptor.clear();

            // Verify the update
            record = records.get(1);
            VerifyRecord.isValidUpdate(record, "id", 1);

            transformed = transform.apply(record);
            after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("id")).isEqualTo(1);
            assertThat(after.get("data")).isEqualTo(data);
            assertThat(after.get("val")).isEqualTo(2);

            // Null will always trigger a re-select
            assertThat(interceptor.containsMessage("No columns require reselect.")).isFalse();
        }
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldNotReselectSpecifiedColumnWhenNullValue() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public\\.dbz4321")
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        LogInterceptor interceptor = new LogInterceptor(ReselectColumns.class);
        interceptor.setLoggerLevel(ReselectColumns.class, Level.DEBUG);

        final String data = RandomStringUtils.randomAlphabetic(4000);
        TestHelper.execute("INSERT INTO dbz4321 values (1,'" + data + "', 1)");

        SourceRecords sourceRecords = consumeRecordsByTopic(1);
        List<SourceRecord> records = sourceRecords.recordsForTopic("test_server.public.dbz4321");
        assertThat(records).hasSize(1);

        Map<String, String> properties = config.asMap();
        properties.put("column.list", "data");

        try (ReselectColumns<SourceRecord> transform = createTransform(properties)) {
            // Verify Insert
            SourceRecord record = records.get(0);
            VerifyRecord.isValidInsert(record, "id", 1);

            SourceRecord transformed = transform.apply(record);
            Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("id")).isEqualTo(1);
            assertThat(after.get("data")).isEqualTo(data);
            assertThat(after.get("val")).isEqualTo(1);
        }

        // Verify no re-selection happened
        assertThat(interceptor.containsMessage("No columns require reselect.")).isTrue();
    }

    @Test
    @FixFor("DBZ-4321")
    public void shouldReselectSpecifiedColumnWhenNullValue() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "public\\.dbz4321")
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        final String data = RandomStringUtils.randomAlphabetic(4000);
        TestHelper.execute("INSERT INTO dbz4321 values (1,'" + data + "', 1)");
        // Update non-Toast
        TestHelper.execute("UPDATE dbz4321 set val=2, data = null where id = 1");

        SourceRecords sourceRecords = consumeRecordsByTopic(2);
        sourceRecords.allRecordsInOrder().forEach(System.out::println);
        List<SourceRecord> records = sourceRecords.recordsForTopic("test_server.public.dbz4321");
        assertThat(records).hasSize(2);

        Map<String, String> properties = config.asMap();
        properties.put("column.list", "data");

        LogInterceptor interceptor = new LogInterceptor(ReselectColumns.class);
        interceptor.setLoggerLevel(ReselectColumns.class, Level.DEBUG);

        try (ReselectColumns<SourceRecord> transform = createTransform(properties)) {
            // Verify the insert
            SourceRecord record = records.get(0);
            VerifyRecord.isValidInsert(record, "id", 1);

            SourceRecord transformed = transform.apply(record);
            Struct after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("id")).isEqualTo(1);
            assertThat(after.get("data")).isEqualTo(data);
            assertThat(after.get("val")).isEqualTo(1);

            // Verify no re-selection happened
            assertThat(interceptor.containsMessage("No columns require reselect.")).isTrue();
            interceptor.clear();

            // Verify the update
            record = records.get(1);
            VerifyRecord.isValidUpdate(record, "id", 1);

            transformed = transform.apply(record);
            after = ((Struct) transformed.value()).getStruct(Envelope.FieldName.AFTER);
            assertThat(after.get("id")).isEqualTo(1);
            assertThat(after.get("data")).isNull();
            assertThat(after.get("val")).isEqualTo(2);

            // Null will always trigger a re-select
            assertThat(interceptor.containsMessage("No columns require reselect.")).isFalse();
        }
    }

    private ReselectColumns<SourceRecord> createTransform(Map<String, String> properties) {
        properties.put("connection.user", "postgres");
        properties.put("connection.password", "postgres");
        properties.put("connection.url", "jdbc:postgresql://localhost:5432/");

        final ReselectColumns<SourceRecord> transform = new ReselectColumns<>();
        transform.configure(properties);
        return transform;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.util.Base64;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.connector.postgresql.junit.SkipWhenDecoderPluginNameIsNot;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * Integration test for logical decoding messages.
 *
 * @author Lairen Hightower
 */
public class LogicalDecodingMessageIT extends AbstractConnectorTest {

    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));";

    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT;

    @Rule
    public final TestRule skipName = new SkipTestDependingOnDecoderPluginNameRule();

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    @FixFor("DBZ-2363")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Only supported on PgOutput")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, minor = 0, reason = "Database Version less than 14")
    public void shouldNotConsumeLogicalDecodingMessagesWhenAllPrefixesAreInTheExcludedList() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST, ".*");
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // emit logical decoding message
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'prefix', 'content');");
        TestHelper.execute("INSERT into s1.a VALUES(201, 1);");

        SourceRecords records = consumeRecordsByTopic(1);
        List<SourceRecord> insertRecords = records.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> logicalMessageRecords = records.recordsForTopic(topicName("message"));
        assertThat(insertRecords).hasSize(1);
        assertNull(logicalMessageRecords);
    }

    @Test
    @FixFor("DBZ-2363")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Only supported on PgOutput")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, minor = 0, reason = "Message not supported for PG version < 14")
    public void shouldConsumeNonTransactionalLogicalDecodingMessages() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig();

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // emit non transactional logical decoding message with text
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'foo', 'bar');");
        // emit non transactional logical decoding message with binary
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'foo', E'bar'::bytea);");

        SourceRecords records = consumeRecordsByTopic(2);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("message"));
        recordsForTopic.forEach(record -> {
            Struct value = (Struct) record.value();
            String op = value.getString(Envelope.FieldName.OPERATION);
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            Struct message = value.getStruct(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY);

            assertNull(source.getInt64(SourceInfo.TXID_KEY));
            assertNotNull(source.getInt64(SourceInfo.TIMESTAMP_KEY));
            assertNotNull(source.getInt64(SourceInfo.LSN_KEY));
            assertEquals("", source.getString(SourceInfo.TABLE_NAME_KEY));
            assertEquals("", source.getString(SourceInfo.SCHEMA_NAME_KEY));

            assertEquals(Envelope.Operation.MESSAGE.code(), op);
            assertEquals("foo",
                    message.getString(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY));
            assertArrayEquals("bar".getBytes(),
                    message.getBytes(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY));
        });
    }

    @Test
    @FixFor("DBZ-2363")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Only supported on PgOutput")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, minor = 0, reason = "Message not supported for PG version < 14")
    public void shouldConsumeTransactionalLogicalDecodingMessages() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig();

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // emit transactional logical decoding message with text
        TestHelper.execute("SELECT pg_logical_emit_message(true, 'txn_foo', 'txn_bar');");
        // emit transactional logical decoding message with binary
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'foo', E'txn_bar'::bytea);");

        SourceRecords txnRecords = consumeRecordsByTopic(1);
        List<SourceRecord> txnRecordsForTopic = txnRecords.recordsForTopic(topicName("message"));
        assertThat(txnRecordsForTopic).hasSize(1);

        txnRecordsForTopic.forEach(record -> {
            Struct value = (Struct) record.value();
            String op = value.getString(Envelope.FieldName.OPERATION);
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            Struct message = value.getStruct(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY);

            assertNotNull(source.getInt64(SourceInfo.TXID_KEY));
            assertNotNull(source.getInt64(SourceInfo.TIMESTAMP_KEY));
            assertNotNull(source.getInt64(SourceInfo.LSN_KEY));
            assertEquals("", source.getString(SourceInfo.TABLE_NAME_KEY));
            assertEquals("", source.getString(SourceInfo.SCHEMA_NAME_KEY));

            assertEquals(Envelope.Operation.MESSAGE.code(), op);
            assertEquals("txn_foo",
                    message.getString(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY));
            assertArrayEquals("txn_bar".getBytes(),
                    message.getBytes(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY));
        });
    }

    @Test
    @FixFor("DBZ-2363")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Only supported on PgOutput")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, minor = 0, reason = "Message not supported for PG version < 14")
    public void shouldApplyBinaryHandlingMode() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.BINARY_HANDLING_MODE, "base64");

        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // emit transactional logical decoding message with binary
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'foo', E'txn_bar'::bytea);");

        SourceRecords txnRecords = consumeRecordsByTopic(1);
        List<SourceRecord> txnRecordsForTopic = txnRecords.recordsForTopic(topicName("message"));
        assertThat(txnRecordsForTopic).hasSize(1);
        SourceRecord record = txnRecordsForTopic.get(0);

        Struct value = (Struct) record.value();
        Struct message = value.getStruct(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY);
        assertThat(new String(Base64.getEncoder().encode("txn_bar".getBytes("UTF-8")), "UTF-8"))
                .isEqualTo(message.getString(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_CONTENT_KEY));
    }

    @Test
    @FixFor("DBZ-2363")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Only supported on PgOutput")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, minor = 0, reason = "Database Version less than 14")
    public void shouldNotConsumeLogicalDecodingMessagesWithExcludedPrefixes() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST, "excluded_prefix, prefix:excluded");
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // emit logical decoding message
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'included_prefix', 'content');");
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'excluded_prefix', 'content');");
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'prefix:excluded', 'content');");
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'prefix:included', 'content');");

        SourceRecords records = consumeRecordsByTopic(2);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("message"));
        assertEquals(2, recordsForTopic.size());

        assertThat(getPrefix(recordsForTopic.get(0))).isEqualTo("included_prefix");
        assertThat(getPrefix(recordsForTopic.get(1))).isEqualTo("prefix:included");
    }

    @Test
    @FixFor("DBZ-2363")
    @SkipWhenDecoderPluginNameIsNot(value = SkipWhenDecoderPluginNameIsNot.DecoderPluginName.PGOUTPUT, reason = "Only supported on PgOutput")
    @SkipWhenDatabaseVersion(check = LESS_THAN, major = 14, minor = 0, reason = "Database Version less than 14")
    public void shouldOnlyConsumeLogicalDecodingMessagesWithIncludedPrefixes() throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST, "included_prefix, prefix:included, ano.*er_included");
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForSnapshotToBeCompleted();

        // emit logical decoding message
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'included_prefix', 'content');");
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'excluded_prefix', 'content');");
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'prefix:excluded', 'content');");
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'prefix:included', 'content');");
        TestHelper.execute("SELECT pg_logical_emit_message(false, 'another_included', 'content');");

        SourceRecords records = consumeRecordsByTopic(3);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topicName("message"));
        assertThat(recordsForTopic).hasSize(3);

        assertThat(((Struct) recordsForTopic.get(0).key()).getString("prefix")).isEqualTo("included_prefix");
        assertThat(getPrefix(recordsForTopic.get(0))).isEqualTo("included_prefix");

        assertThat(((Struct) recordsForTopic.get(1).key()).getString("prefix")).isEqualTo("prefix:included");
        assertThat(getPrefix(recordsForTopic.get(1))).isEqualTo("prefix:included");

        assertThat(((Struct) recordsForTopic.get(2).key()).getString("prefix")).isEqualTo("another_included");
        assertThat(getPrefix(recordsForTopic.get(2))).isEqualTo("another_included");
    }

    private String getPrefix(SourceRecord record) {
        Struct message = ((Struct) record.value()).getStruct(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_KEY);
        return message.getString(LogicalDecodingMessageMonitor.DEBEZIUM_LOGICAL_DECODING_MESSAGE_PREFIX_KEY);
    }

    private void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);
    }
}

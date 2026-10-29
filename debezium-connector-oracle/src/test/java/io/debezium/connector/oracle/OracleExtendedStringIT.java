/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipOnDatabaseParameter;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.IoUtil;
import io.debezium.util.Testing;

/**
 * Integration tests for when MAX_STRING_SIZE is set to EXTENDED.
 *
 * @author Chris Cranford
 */
@SkipOnDatabaseParameter(parameterName = "max_string_size", value = "EXTENDED", matches = false, reason = "Requires max_string_size set to EXTENDED")
public class OracleExtendedStringIT extends AbstractAsyncEngineConnectorTest {

    private OracleConnection connection;

    @BeforeEach
    void beforeEach() throws Exception {
        this.connection = TestHelper.testConnection();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() throws Exception {
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

    @Test
    @FixFor({ "DBZ-8034", "DBZ-8200" })
    public void testRelaxedQuoteDetectionForExtendedStrings() throws Exception {
        TestHelper.dropTable(connection, "dbz8034");
        try {
            // Creates a table with extended string support
            connection.execute("CREATE TABLE dbz8034 (id numeric(9,0) primary key, data1 varchar2(8000), data2 numeric(9,0))");
            TestHelper.streamTable(connection, "dbz8034");

            final String snapshotData = "C'est une belle journée.";
            connection.prepareUpdate("INSERT INTO dbz8034 (id,data1,data2) values (0,?,10)", ps -> ps.setString(1, snapshotData));
            connection.commit();

            final Configuration config = defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8034")
                    .with(OracleConnectorConfig.LOG_MINING_SQL_RELAXED_QUOTE_DETECTION, "true")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String insertData = "C'est une belle journée.";
            connection.prepareUpdate("INSERT INTO dbz8034 (id,data1,data2) values (1,?,10)", ps -> ps.setString(1, insertData));
            connection.commit();

            final String updateData = "C'est mon livre préféré.";
            connection.prepareUpdate("UPDATE dbz8034 SET data1=?, data2=20 WHERE id=1", ps -> ps.setString(1, updateData));
            connection.commit();

            connection.execute("DELETE FROM dbz8034 WHERE id=1");

            List<SourceRecord> records = consumeRecordsByTopic(5).recordsForTopic(topicName("DBZ8034"));
            assertThat(records).hasSize(5);

            SourceRecord record = records.get(0);
            VerifyRecord.isValidRead(record, "ID", 0);
            assertThat(getAfter(record).get("DATA1")).isEqualTo(snapshotData);

            record = records.get(1);
            VerifyRecord.isValidInsert(record, "ID", 1);
            assertThat(getAfter(record).get("DATA1")).isEqualTo(insertData);

            // note: this exists because in this use case, the transaction commit consumer cannot
            // merge the two UPDATE redo entries due to insufficient content in the entries to
            // know it's safe to merge them.
            record = records.get(2);
            VerifyRecord.isValidUpdate(record, "ID", 1);
            assertThat(getAfter(record).get("DATA1")).isEqualTo("__debezium_unavailable_value");

            record = records.get(3);
            VerifyRecord.isValidUpdate(record, "ID", 1);
            assertThat(getAfter(record).get("DATA1")).isEqualTo(updateData);

            record = records.get(4);
            VerifyRecord.isValidDelete(record, "ID", 1);
        }
        finally {
            TestHelper.dropTable(connection, "dbz8034");
        }
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldStreamExtendedStringColumnWithCharLengthSemanticsFromSnapshot() throws Exception {
        TestHelper.dropTable(connection, "dbz2794");
        try {
            // The declared length is under 4000, but the byte length is not on multibyte character sets
            connection.execute("CREATE TABLE dbz2794 (id numeric(9,0) primary key, txt varchar2(2000 char))");
            TestHelper.streamTable(connection, "dbz2794");
            assumeExtendedByteLength("DBZ2794", "TXT");

            connection.execute("INSERT INTO dbz2794 (id,txt) values (1,'x')");

            start(OracleConnector.class, defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2794").build());
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            assertCharLengthSemanticsChanges("DBZ2794", true);
        }
        finally {
            TestHelper.dropTable(connection, "dbz2794");
        }
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldStreamExtendedStringColumnWithCharLengthSemanticsCreatedWhileStreaming() throws Exception {
        TestHelper.dropTable(connection, "dbz2794");
        try {
            start(OracleConnector.class, defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2794").build());
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // The table is registered from the DDL, so the byte length comes from the DDL parser
            connection.execute("CREATE TABLE dbz2794 (id numeric(9,0) primary key, txt varchar2(2000 char))");
            TestHelper.streamTable(connection, "dbz2794");
            assumeExtendedByteLength("DBZ2794", "TXT");

            connection.execute("INSERT INTO dbz2794 (id,txt) values (1,'x')");

            assertCharLengthSemanticsChanges("DBZ2794", false);
        }
        finally {
            TestHelper.dropTable(connection, "dbz2794");
        }
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldStreamExtendedNationalStringColumn() throws Exception {
        TestHelper.dropTable(connection, "dbz2794");
        try {
            // 4002 bytes with the AL16UTF16 national character set
            connection.execute("CREATE TABLE dbz2794 (id numeric(9,0) primary key, txt nvarchar2(2001))");
            TestHelper.streamTable(connection, "dbz2794");
            assumeExtendedByteLength("DBZ2794", "TXT");

            connection.execute("INSERT INTO dbz2794 (id,txt) values (1,N'x')");

            start(OracleConnector.class, defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2794").build());
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = RandomStringUtils.randomAlphanumeric(2001);
            connection.prepareUpdate("UPDATE dbz2794 SET txt=? WHERE id=1", ps -> ps.setNString(1, data));
            connection.commit();

            final List<SourceRecord> records = consumeRecordsByTopic(2).recordsForTopic(topicName("DBZ2794"));
            assertThat(records).hasSize(2);

            VerifyRecord.isValidRead(records.get(0), "ID", 1);
            assertThat(getAfter(records.get(0)).get("TXT")).isEqualTo("x");

            VerifyRecord.isValidUpdate(records.get(1), "ID", 1);
            assertThat(getAfter(records.get(1)).get("TXT")).isEqualTo(data);
        }
        finally {
            TestHelper.dropTable(connection, "dbz2794");
        }
    }

    @Test
    @Disabled("The standard test images use the AL16UTF16 national character set by default")
    @FixFor("debezium/dbz#2794")
    public void shouldStreamExtendedNationalStringColumnWithUtf8NationalCharacterSet() throws Exception {
        TestHelper.dropTable(connection, "dbz2794");
        try {
            // The declared length is under the old 2000 character rule, but with a UTF8 national character set
            // the byte length is 4500, so only the byte-based rule detects it; skipped on AL16UTF16 (3000 bytes)
            connection.execute("CREATE TABLE dbz2794 (id numeric(9,0) primary key, txt nvarchar2(1500))");
            TestHelper.streamTable(connection, "dbz2794");
            assumeExtendedByteLength("DBZ2794", "TXT");

            connection.execute("INSERT INTO dbz2794 (id,txt) values (1,N'x')");

            start(OracleConnector.class, defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2794").build());
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            final String data = RandomStringUtils.randomAlphanumeric(1500);
            connection.prepareUpdate("UPDATE dbz2794 SET txt=? WHERE id=1", ps -> ps.setNString(1, data));
            connection.commit();

            final List<SourceRecord> records = consumeRecordsByTopic(2).recordsForTopic(topicName("DBZ2794"));
            assertThat(records).hasSize(2);

            VerifyRecord.isValidRead(records.get(0), "ID", 1);
            assertThat(getAfter(records.get(0)).get("TXT")).isEqualTo("x");

            VerifyRecord.isValidUpdate(records.get(1), "ID", 1);
            assertThat(getAfter(records.get(1)).get("TXT")).isEqualTo(data);
        }
        finally {
            TestHelper.dropTable(connection, "dbz2794");
        }
    }

    @Test
    @FixFor("debezium/dbz#2794")
    public void shouldCaptureExtendedStringColumnAfterRecoverySnapshotOfLegacySchemaHistory() throws Exception {
        TestHelper.dropTable(connection, "dbz2794");
        try {
            connection.execute("CREATE TABLE dbz2794 (id numeric(9,0) primary key, txt varchar2(2000 char))");
            TestHelper.streamTable(connection, "dbz2794");
            assumeExtendedByteLength("DBZ2794", "TXT");

            connection.execute("INSERT INTO dbz2794 (id,txt) values (1,'x')");

            final Configuration config = defaultConfig().with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2794").build();
            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            assertThat(consumeRecordsByTopic(1).recordsForTopic(topicName("DBZ2794"))).hasSize(1);
            stopConnector();

            // Rewrite the schema history as an older connector version wrote it, without the Oracle column entries
            final File historyFile = TestHelper.SCHEMA_HISTORY_PATH.toFile();
            assertThat(IoUtil.read(historyFile)).contains("\"octetLength\"");
            final List<String> historyLines = new ArrayList<>();
            IoUtil.readLines(TestHelper.SCHEMA_HISTORY_PATH, historyLines::add);
            try (Writer writer = new FileWriter(historyFile, StandardCharsets.UTF_8)) {
                for (String line : historyLines) {
                    final Document historyRecord = DocumentReader.defaultReader().read(line);
                    final Array tableChanges = historyRecord.getArray("tableChanges");
                    if (tableChanges != null) {
                        tableChanges.streamValues()
                                .map(change -> change.asDocument().getDocument("table"))
                                .filter(Objects::nonNull)
                                .flatMap(table -> table.getArray("columns").streamValues())
                                .forEach(column -> column.asDocument().remove("oracle"));
                    }
                    writer.write(DocumentWriter.defaultWriter().write(historyRecord));
                    writer.write('\n');
                }
            }
            assertThat(IoUtil.read(historyFile)).doesNotContain("\"octetLength\"");

            // Restarting from the legacy history logs the warning about the missing byte length
            final LogInterceptor logInterceptor = new LogInterceptor(OracleDatabaseSchema.class);
            start(OracleConnector.class, config);
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertThat(logInterceptor.containsWarnMessage("has no byte length for column 'TXT'")).isTrue();
            stopConnector();

            // The recovery snapshot rebuilds the schema history with the byte length
            start(OracleConnector.class, config.edit().with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.RECOVERY).build());
            assertConnectorIsRunning();
            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
            assertThat(IoUtil.read(historyFile)).contains("\"octetLength\"");

            connection.execute("UPDATE dbz2794 SET txt = RPAD('Debezium test ', 2000, TO_CHAR(UNISTR('\\20AC'))) WHERE id = 1");

            final List<SourceRecord> records = consumeRecordsByTopic(1).recordsForTopic(topicName("DBZ2794"));
            assertThat(records).hasSize(1);
            VerifyRecord.isValidUpdate(records.get(0), "ID", 1);
            assertThat(getAfter(records.get(0)).get("TXT")).isEqualTo("Debezium test " + "€".repeat(2000 - "Debezium test ".length()));
        }
        finally {
            TestHelper.dropTable(connection, "dbz2794");
        }
    }

    private void assertCharLengthSemanticsChanges(String tableName, boolean fromSnapshot) throws Exception {
        // About 6000 bytes in AL32UTF8, but only 2000 characters
        connection.execute("UPDATE " + tableName + " SET txt = RPAD('Debezium test ', 2000, TO_CHAR(UNISTR('\\20AC'))) WHERE id = 1");
        // A short value is also written through the extended string operations
        connection.execute("UPDATE " + tableName + " SET txt = 'short' WHERE id = 1");

        final String expected = "Debezium test " + "€".repeat(2000 - "Debezium test ".length());

        final List<SourceRecord> records = consumeRecordsByTopic(3).recordsForTopic(topicName(tableName));
        assertThat(records).hasSize(3);

        if (fromSnapshot) {
            VerifyRecord.isValidRead(records.get(0), "ID", 1);
        }
        else {
            VerifyRecord.isValidInsert(records.get(0), "ID", 1);
        }
        assertThat(getAfter(records.get(0)).get("TXT")).isEqualTo("x");

        VerifyRecord.isValidUpdate(records.get(1), "ID", 1);
        assertUnavailableValue(getBefore(records.get(1)).get("TXT"));
        assertThat(getAfter(records.get(1)).get("TXT")).isEqualTo(expected);

        VerifyRecord.isValidUpdate(records.get(2), "ID", 1);
        assertThat(getAfter(records.get(2)).get("TXT")).isEqualTo("short");
    }

    private void assumeExtendedByteLength(String tableName, String columnName) throws Exception {
        // The character sets decide the byte length, so skip on databases where the column is not extended
        final int dataLength = connection.prepareQueryAndMap(
                "SELECT DATA_LENGTH FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?",
                ps -> {
                    ps.setString(1, tableName);
                    ps.setString(2, columnName);
                },
                rs -> rs.next() ? rs.getInt(1) : 0);
        Assumptions.assumeTrue(dataLength > 4000, "Column byte length " + dataLength + " is not an extended string");
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

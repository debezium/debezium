/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.DataQueryMode;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.util.Testing;

/**
 * Integration tests for max-type columns ({@code varchar(max)} and {@code varbinary(max)}) in
 * UPDATE events.
 * <p>
 * SQL Server records no before-image value for a max-type column an update did not touch, and
 * records a null for one the update set to null. Only the {@code __$update_mask} bits tell the two
 * apart, so an untouched column is reported with the unavailable value placeholder while a column
 * the update did change keeps its recorded value, including null. The mask spans more than one
 * byte only once a capture instance covers more than eight columns, which is why the table used
 * here is deliberately wider than that.
 */
public class SqlServerMaxColumnPlaceholderIT extends AbstractAsyncEngineConnectorTest {

    private static final String DOCUMENTS = "documents";
    private static final String LEGAL_TEXT = "legal_text";
    private static final String PAYLOAD = "payload";
    private static final String STATUS = "status";
    private static final String OPERATION = "__$operation";
    private static final String PLACEHOLDER = RelationalDatabaseConnectorConfig.DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER;
    private static final byte[] PAYLOAD_VALUE = { 0x01, 0x02 };

    // Twelve captured columns, so the update mask needs two bytes and the max-type columns sit in
    // the byte that SQL Server writes last.
    private static final String DOCUMENTS_DDL = "CREATE TABLE dbo.documents ("
            + "id INT NOT NULL PRIMARY KEY, "
            + "legal_text VARCHAR(MAX) NULL, "
            + "notes NVARCHAR(MAX) NULL, "
            + "payload VARBINARY(MAX) NULL, "
            + "status VARCHAR(20) NULL, "
            + "page_count INT NULL, "
            + "revision INT NULL, "
            + "owner_id INT NULL, "
            + "department_id INT NULL, "
            + "retention_years INT NULL, "
            + "reviewer VARCHAR(50) NULL, "
            + "archived BIT NULL);";

    private static final String INSERT_DOCUMENT = "INSERT INTO dbo.documents (id, legal_text, notes, payload, status) "
            + "VALUES (1, NULL, N'review notes', 0x0102, 'NEW');";
    private static final String SET_LEGAL_TEXT = "UPDATE dbo.documents SET legal_text = 'first value' WHERE id = 1;";
    private static final String CLEAR_LEGAL_TEXT = "UPDATE dbo.documents SET legal_text = NULL WHERE id = 1;";
    private static final String CHANGE_STATUS = "UPDATE dbo.documents SET status = 'PROCESSED' WHERE id = 1;";

    private SqlServerConnection connection;

    @BeforeEach
    public void before() throws SQLException, InterruptedException {
        TestHelper.createTestDatabase();
        connection = TestHelper.testConnection();
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        connection.execute(DOCUMENTS_DDL);
        TestHelper.enableTableCdc(connection, DOCUMENTS);
        Thread.sleep(Duration.ofSeconds(TestHelper.waitTimeForLsnTimeMapping()).toMillis());
    }

    @AfterEach
    public void after() throws SQLException {
        stopConnector();
        if (connection != null) {
            connection.close();
        }
    }

    @ParameterizedTest(name = "data.query.mode = {0}")
    @EnumSource(DataQueryMode.class)
    @FixFor("dbz#2650")
    void shouldEmitRecordedValueForMaxColumnChangedByUpdate(DataQueryMode mode) throws Exception {
        start(SqlServerConnector.class, config(mode));
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();

        connection.execute(INSERT_DOCUMENT);
        connection.execute(SET_LEGAL_TEXT);
        connection.execute(CLEAR_LEGAL_TEXT);

        // the before image of the last update is the capture process' final write for this row
        TestHelper.waitForCdcRecord(connection, DOCUMENTS,
                rs -> rs.getInt(OPERATION) == SqlServerChangeRecordEmitter.OP_UPDATE_BEFORE
                        && "first value".equals(rs.getString(LEGAL_TEXT)));

        final List<SourceRecord> records = consumeDocuments(3);
        assertThat(records).hasSize(3);

        // Setting the column for the first time: its before image is the null the row really held.
        final SourceRecord textSet = records.get(1);
        assertThat(before(textSet).getString(LEGAL_TEXT)).isNull();
        assertThat(after(textSet).getString(LEGAL_TEXT)).isEqualTo("first value");

        // Clearing the column: its after image is the null the update really wrote.
        final SourceRecord textCleared = records.get(2);
        assertThat(before(textCleared).getString(LEGAL_TEXT)).isEqualTo("first value");
        assertThat(after(textCleared).getString(LEGAL_TEXT)).isNull();

        // A max column the same update left alone keeps its placeholder before image, while its
        // after image carries the value SQL Server recorded.
        assertThat(binaryOf(before(textSet), PAYLOAD)).isEqualTo(PLACEHOLDER.getBytes());
        assertThat(binaryOf(after(textSet), PAYLOAD)).isEqualTo(PAYLOAD_VALUE);
    }

    @ParameterizedTest(name = "data.query.mode = {0}")
    @EnumSource(DataQueryMode.class)
    @FixFor("dbz#1164")
    void shouldEmitPlaceholderForMaxColumnUntouchedByUpdate(DataQueryMode mode) throws Exception {
        start(SqlServerConnector.class, config(mode));
        assertConnectorIsRunning();
        TestHelper.waitForStreamingStarted();

        connection.execute(INSERT_DOCUMENT);
        connection.execute(CHANGE_STATUS);

        TestHelper.waitForCdcRecord(connection, DOCUMENTS,
                rs -> rs.getInt(OPERATION) == SqlServerChangeRecordEmitter.OP_UPDATE_AFTER
                        && "PROCESSED".equals(rs.getString(STATUS)));

        final List<SourceRecord> records = consumeDocuments(2);
        assertThat(records).hasSize(2);

        final SourceRecord statusChanged = records.get(1);
        assertThat(before(statusChanged).getString(STATUS)).isEqualTo("NEW");
        assertThat(after(statusChanged).getString(STATUS)).isEqualTo("PROCESSED");

        // legal_text is null in the row and took no part in the update, so neither image may report
        // it as a null this update wrote.
        assertThat(before(statusChanged).getString(LEGAL_TEXT)).isEqualTo(PLACEHOLDER);
        assertThat(after(statusChanged).getString(LEGAL_TEXT)).isEqualTo(PLACEHOLDER);

        // payload holds a value the update did not touch, so only its before image is reported as
        // unavailable.
        assertThat(binaryOf(before(statusChanged), PAYLOAD)).isEqualTo(PLACEHOLDER.getBytes());
        assertThat(binaryOf(after(statusChanged), PAYLOAD)).isEqualTo(PAYLOAD_VALUE);
    }

    private Configuration config(DataQueryMode mode) {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(SqlServerConnectorConfig.DATA_QUERY_MODE, mode)
                .with(SqlServerConnectorConfig.TABLE_INCLUDE_LIST, "dbo." + DOCUMENTS)
                .build();
    }

    private List<SourceRecord> consumeDocuments(int count) throws InterruptedException {
        return consumeRecordsByTopic(count).recordsForTopic(TestHelper.topicName(TestHelper.TEST_DATABASE_1, DOCUMENTS));
    }

    private Struct before(SourceRecord record) {
        return (Struct) ((Struct) record.value()).get(Envelope.FieldName.BEFORE);
    }

    private Struct after(SourceRecord record) {
        return (Struct) ((Struct) record.value()).get(Envelope.FieldName.AFTER);
    }

    private byte[] binaryOf(Struct image, String fieldName) {
        final Object value = image.get(fieldName);
        if (value instanceof ByteBuffer) {
            final ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            final byte[] content = new byte[buffer.remaining()];
            buffer.get(content);
            return content;
        }
        return (byte[]) value;
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.util.Testing;

/**
 * Verifies that streaming resumes correctly when the connector is restarted from the position of a chunked row
 * LCR, that is a row whose XMLTYPE or LOB data was delivered through the chunk callback.
 * <p>
 * The record of such a row carries the position of the row LCR, while the row's trailing pieces occupy later
 * positions within the same {commit SCN, SCN} tuple. When that record's offset is the last one Kafka Connect
 * stored, attaching the outbound server at the stored position made it deliver the trailing pieces on their own,
 * for example an {@code XML WRITE} with no column values that the {@code LcrEventHandler} cannot emit, or a
 * {@code LOB WRITE} or {@code UPDATE} that was emitted as a second change for a row already sent. The connector
 * now attaches at the start of the stored position's tuple and publishes that same position as the processed low
 * watermark, so the outbound server re-sends the row complete with its chunks and the handler skips it as already
 * processed.
 * <p>
 * The scenarios cover an XMLTYPE insert, an XMLTYPE update, a CLOB update, and a stop inside a transaction of five
 * XMLTYPE rows, each with {@code provide.transaction.metadata} disabled and enabled. With it enabled, a stop between
 * transactions stores the END record's commit position, so nothing of the row is re-sent; a stop inside a
 * transaction stores a row position either way. In every case the connector must keep running after the restart
 * and the data topic must receive only the rows that were not emitted before the stop.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.XSTREAM)
public class OracleXStreamChunkedRowRestartIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC = TestHelper.SERVER_NAME + ".DEBEZIUM.DBZ2782";
    private static final String TRANSACTION_TOPIC = TestHelper.SERVER_NAME + ".transaction";

    private OracleConnection connection;

    @BeforeEach
    void beforeEach() {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        TestHelper.dropTable(connection, "dbz2782");
    }

    @AfterEach
    void afterEach() throws Exception {
        if (connection != null) {
            TestHelper.dropTable(connection, "dbz2782");
            connection.close();
        }
    }

    @ParameterizedTest(name = "transactionMetadata={0}")
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#2782")
    public void shouldResumeAfterStopOnChunkedXmlTypeInsert(boolean transactionMetadata) throws Exception {
        connection.execute("CREATE TABLE DBZ2782 (ID numeric(9,0), DATA xmltype, primary key(ID)) XMLTYPE COLUMN DATA STORE AS BINARY XML");
        shouldResumeAfterStop(false, false, transactionMetadata);
    }

    @ParameterizedTest(name = "transactionMetadata={0}")
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#2782")
    public void shouldResumeAfterStopOnChunkedXmlTypeUpdate(boolean transactionMetadata) throws Exception {
        connection.execute("CREATE TABLE DBZ2782 (ID numeric(9,0), DATA xmltype, primary key(ID)) XMLTYPE COLUMN DATA STORE AS BINARY XML");
        shouldResumeAfterStop(true, false, transactionMetadata);
    }

    @ParameterizedTest(name = "transactionMetadata={0}")
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#2782")
    public void shouldResumeAfterStopOnChunkedClobUpdate(boolean transactionMetadata) throws Exception {
        connection.execute("CREATE TABLE DBZ2782 (ID numeric(9,0), DATA clob, primary key(ID))");
        shouldResumeAfterStop(true, true, transactionMetadata);
    }

    @ParameterizedTest(name = "transactionMetadata={0}")
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#2782")
    public void shouldResumeAfterStopInsideTransactionWithChunkedRows(boolean transactionMetadata) throws Exception {
        connection.execute("CREATE TABLE DBZ2782 (ID numeric(9,0), DATA xmltype, primary key(ID)) XMLTYPE COLUMN DATA STORE AS BINARY XML");
        TestHelper.streamTable(connection, "dbz2782");

        final Configuration config = config(transactionMetadata);

        // Stop the engine as the fourth row reaches the consumer, so the third row's position is stored
        start(OracleConnector.class, config, record -> TOPIC.equals(record.topic()) && Integer.valueOf(4).equals(after(record).get("ID")));
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        for (int id = 1; id <= 5; id++) {
            insertRow(false, id);
        }
        connection.commit();

        Awaitility.await().atMost(Duration.ofSeconds(TestHelper.defaultMessageConsumerPollTimeout())).until(() -> !isEngineRunning.get());
        SourceRecords records = consumeAvailableRecordsByTopic();
        assertInserts(records.recordsForTopic(TOPIC), 1, 2, 3);
        assertTransactionRecords(records, transactionMetadata ? List.of("BEGIN") : List.of());
        stopConnector();

        start(OracleConnector.class, config);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        records = consumeRecordsByTopic(transactionMetadata ? 3 : 2);
        assertInserts(records.recordsForTopic(TOPIC), 4, 5);
        assertTransactionRecords(records, transactionMetadata ? List.of("END") : List.of());
        assertNothingElseArrives();

        stopConnector();
    }

    /**
     * One DML on a row with chunked data, consume its record, stop the connector with no heartbeat or later
     * record, then restart from the stored offset and insert a second row.
     */
    private void shouldResumeAfterStop(boolean update, boolean clob, boolean transactionMetadata) throws Exception {
        final int recordsPerTransaction = transactionMetadata ? 3 : 1;
        TestHelper.streamTable(connection, "dbz2782");

        if (update) {
            insertRow(clob, 1);
            connection.commit();
        }

        final Configuration config = config(transactionMetadata);

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        if (update) {
            final String payload = buildPayload(1);
            final String sql = clob ? "UPDATE dbz2782 SET data = ? WHERE id = 1" : "UPDATE dbz2782 SET data = xmltype(?) WHERE id = 1";
            connection.prepareQuery(sql, ps -> ps.setObject(1, payload), null);
        }
        else {
            insertRow(clob, 1);
        }
        connection.commit();

        SourceRecords records = consumeRecordsByTopic(recordsPerTransaction);
        final List<SourceRecord> dataRecords = records.recordsForTopic(TOPIC);
        assertThat(dataRecords).hasSize(1);
        if (update) {
            VerifyRecord.isValidUpdate(dataRecords.get(0), "ID", 1);
        }
        else {
            VerifyRecord.isValidInsert(dataRecords.get(0), "ID", 1);
        }
        assertPayload(dataRecords.get(0), 1);
        assertTransactionRecords(records, transactionMetadata ? List.of("BEGIN", "END") : List.of());

        stopConnector();

        start(OracleConnector.class, config);
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        insertRow(clob, 2);
        connection.commit();

        // With transaction metadata, the offset restored from the END record still describes its transaction
        // as in progress, so the TransactionMonitor closes it with an extra END before the new BEGIN.
        records = consumeRecordsByTopic(transactionMetadata ? 4 : 1);
        assertInserts(records.recordsForTopic(TOPIC), 2);
        assertTransactionRecords(records, transactionMetadata ? List.of("END", "BEGIN", "END") : List.of());
        assertNothingElseArrives();

        stopConnector();
    }

    private static Configuration config(boolean transactionMetadata) {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2782")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, transactionMetadata)
                .build();
    }

    private void insertRow(boolean clob, int id) throws Exception {
        final String payload = buildPayload(id);
        final String sql = clob ? "INSERT INTO dbz2782 VALUES (" + id + ", ?)" : "INSERT INTO dbz2782 VALUES (" + id + ", xmltype(?))";
        connection.prepareQuery(sql, ps -> ps.setObject(1, payload), null);
    }

    private void assertNothingElseArrives() throws InterruptedException {
        waitForAvailableRecords(10, TimeUnit.SECONDS);
        assertNoRecordsToConsume();
        assertConnectorIsRunning();
    }

    private static void assertInserts(List<SourceRecord> records, int... ids) {
        assertThat(records).isNotNull().hasSize(ids.length);
        for (int i = 0; i < ids.length; i++) {
            VerifyRecord.isValidInsert(records.get(i), "ID", ids[i]);
            assertPayload(records.get(i), ids[i]);
        }
    }

    private static void assertPayload(SourceRecord record, int id) {
        assertThat(after(record).getString("DATA")).contains("<c40>value-" + id + "-40-abcdefghijklmnop</c40>");
    }

    private static void assertTransactionRecords(SourceRecords records, List<String> statuses) {
        final List<SourceRecord> transactionRecords = records.recordsForTopic(TRANSACTION_TOPIC);
        if (statuses.isEmpty()) {
            assertThat(transactionRecords).isNull();
            return;
        }
        assertThat(transactionRecords).hasSize(statuses.size());
        for (int i = 0; i < statuses.size(); i++) {
            assertThat(((Struct) transactionRecords.get(i).value()).getString(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY))
                    .isEqualTo(statuses.get(i));
        }
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    /**
     * Roughly the reporter's document: 75 elements, 40 of them populated, about 1.5 KB.
     */
    private static String buildPayload(int seed) {
        final StringBuilder sb = new StringBuilder("<row id=\"").append(seed).append("\">");
        for (int i = 1; i <= 75; i++) {
            sb.append("<c").append(i).append(">");
            if (i <= 40) {
                sb.append("value-").append(seed).append("-").append(i).append("-abcdefghijklmnop");
            }
            sb.append("</c").append(i).append(">");
        }
        return sb.append("</row>").toString();
    }
}

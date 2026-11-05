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

import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Testing;

/**
 * Verifies how the XStream adapter handles a row LCR that supplies columns the connector's relational model of
 * the table does not have, on both the regular LCR callback and the chunk callback used for LOB data.
 * <p>
 * The connector's schema is made stale by dropping a captured table and re-creating it with
 * {@code CREATE TABLE ... AS SELECT}, whose column definitions the DDL parser cannot resolve, so the relational
 * model of the re-created table has no columns (see debezium/dbz#801). If that limitation is lifted, these
 * tests need another way to make the schema stale.
 * <p>
 * Such an event must never be emitted partially. With {@code event.processing.failure.handling.mode=fail} the
 * connector stops with an error naming the table and the unknown columns, rather than a
 * {@code NullPointerException} or, on the chunk callback, an opaque {@code ORA-26824} from the XStream client.
 * With {@code warn} or {@code skip} the event is skipped and streaming continues.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.XSTREAM)
public class OracleXStreamUnknownColumnIT extends AbstractAsyncEngineConnectorTest {

    private static final String TOPIC = TestHelper.SERVER_NAME + ".DEBEZIUM.DBZ1867";
    private static final String OTHER_TOPIC = TestHelper.SERVER_NAME + ".DEBEZIUM.DBZ1867_OTHER";

    private OracleConnection connection;

    @BeforeEach
    void beforeEach() {
        connection = TestHelper.testConnection();
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
        TestHelper.dropTables(connection, "dbz1867", "dbz1867_src", "dbz1867_other");
    }

    @AfterEach
    void afterEach() throws Exception {
        if (connection != null) {
            TestHelper.dropTables(connection, "dbz1867", "dbz1867_src", "dbz1867_other");
            connection.close();
        }
    }

    @ParameterizedTest(name = "clob={0}")
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#1867")
    public void shouldStopWithDescriptiveErrorWhenLcrHasColumnNotInSchema(boolean clob) throws Exception {
        final LogInterceptor handlerLogs = new LogInterceptor(LcrEventHandler.class);
        final LogInterceptor errorLogs = new LogInterceptor(ErrorHandler.class);

        startAndRecreateTableWithStaleSchema(clob, EventProcessingFailureHandlingMode.FAIL);

        insertRow(clob, 2);
        connection.commit();

        Awaitility.await()
                .atMost(Duration.ofSeconds(TestHelper.defaultMessageConsumerPollTimeout()))
                .until(() -> !isEngineRunning.get());

        assertThat(handlerLogs.containsErrorMessage("has columns [ID, DATA] that are not in the connector's schema")).isTrue();
        assertThat(errorLogs.containsStacktraceElement("DEBEZIUM.DBZ1867")).isTrue();
        assertThat(errorLogs.containsThrowableWithCause(NullPointerException.class)).isFalse();
        // An exception escaping the chunk callback surfaces as ORA-26824 from the native XStream client
        assertThat(errorLogs.containsStacktraceElement("ORA-26824")).isFalse();

        final SourceRecords records = consumeAvailableRecordsByTopic();
        assertThat(records.recordsForTopic(TOPIC)).isNull();
    }

    @ParameterizedTest(name = "clob={0}")
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#1867")
    public void shouldWarnAndSkipEventWhenLcrHasColumnNotInSchema(boolean clob) throws Exception {
        final LogInterceptor handlerLogs = new LogInterceptor(LcrEventHandler.class);

        startAndRecreateTableWithStaleSchema(clob, EventProcessingFailureHandlingMode.WARN);
        assertSkippedAndStreamingContinues(clob);

        assertThat(handlerLogs.containsWarnMessage("has columns [ID, DATA] that are not in the connector's schema")).isTrue();
    }

    @ParameterizedTest(name = "clob={0}")
    @ValueSource(booleans = { false, true })
    @FixFor("debezium/dbz#1867")
    public void shouldSilentlySkipEventWhenLcrHasColumnNotInSchema(boolean clob) throws Exception {
        final LogInterceptor handlerLogs = new LogInterceptor(LcrEventHandler.class);

        startAndRecreateTableWithStaleSchema(clob, EventProcessingFailureHandlingMode.SKIP);
        assertSkippedAndStreamingContinues(clob);

        assertThat(handlerLogs.containsWarnMessage("that are not in the connector's schema")).isFalse();
        assertThat(handlerLogs.containsErrorMessage("that are not in the connector's schema")).isFalse();
    }

    private void startAndRecreateTableWithStaleSchema(boolean clob, EventProcessingFailureHandlingMode mode) throws Exception {
        final String dataType = clob ? "clob" : "varchar2(50)";
        connection.execute("CREATE TABLE dbz1867_src (id numeric(9,0), data " + dataType + ")");
        connection.execute("CREATE TABLE dbz1867 (id numeric(9,0), data " + dataType + ")");
        connection.execute("CREATE TABLE dbz1867_other (id numeric(9,0) primary key, data varchar2(50))");
        TestHelper.streamTable(connection, "dbz1867");
        TestHelper.streamTable(connection, "dbz1867_other");
        insertRow(clob, "dbz1867_src", 1);
        connection.commit();

        final Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1867,DEBEZIUM\\.DBZ1867_OTHER")
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .with(CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, mode.getValue())
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();
        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        // The DDL parser cannot resolve the columns of a CTAS, so the re-created table has none (dbz#801)
        connection.execute("DROP TABLE dbz1867 PURGE");
        connection.execute("CREATE TABLE dbz1867 AS SELECT * FROM dbz1867_src");
        TestHelper.streamTable(connection, "dbz1867");
    }

    private void assertSkippedAndStreamingContinues(boolean clob) throws Exception {
        insertRow(clob, 2);
        connection.commit();

        connection.execute("INSERT INTO dbz1867_other VALUES (1, 'after the skipped event')");

        final SourceRecords records = consumeRecordsByTopic(1);
        assertThat(records.recordsForTopic(TOPIC)).isNull();
        final List<SourceRecord> otherRecords = records.recordsForTopic(OTHER_TOPIC);
        assertThat(otherRecords).hasSize(1);
        VerifyRecord.isValidInsert(otherRecords.get(0), "ID", 1);

        waitForAvailableRecords(10, TimeUnit.SECONDS);
        assertNoRecordsToConsume();
        assertConnectorIsRunning();

        stopConnector();
    }

    private void insertRow(boolean clob, int id) throws Exception {
        insertRow(clob, "dbz1867", id);
    }

    private void insertRow(boolean clob, String table, int id) throws Exception {
        // A CLOB value is delivered through the chunk callback, a VARCHAR2 value with the row LCR itself
        final String payload = clob ? "clob-" + id + "-" + "x".repeat(2000) : "value-" + id;
        connection.prepareQuery("INSERT INTO " + table + " VALUES (" + id + ", ?)", ps -> ps.setObject(1, payload), null);
    }
}

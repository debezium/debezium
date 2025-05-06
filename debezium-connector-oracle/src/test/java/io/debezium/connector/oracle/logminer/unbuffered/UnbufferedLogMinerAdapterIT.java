/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

import ch.qos.logback.classic.Level;

/**
 * Tests specific to the Oracle LogMiner unbuffered adapter implementation.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_UNBUFFERED)
public class UnbufferedLogMinerAdapterIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnection connection;

    @Before
    public void beforeEach() throws SQLException {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropAllTables();

        connection = TestHelper.testConnection();
    }

    @After
    public void afterEach() throws SQLException {
        stopConnector();

        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-9013")
    public void shouldAdvanceOffsetLowWatermarkWhenWhenNoInProgressTransactionsExist() throws Exception {
        TestHelper.dropTable(connection, "dbz9013");
        try {
            connection.execute("CREATE TABLE dbz9013 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz9013");

            final LogInterceptor resumePositionLogInterceptor = new LogInterceptor(ResumePositionProvider.class);
            resumePositionLogInterceptor.setLoggerLevel(ResumePositionProvider.class, Level.DEBUG);

            final LogInterceptor sourceLogInterceptor = new LogInterceptor(UnbufferedLogMinerStreamingChangeEventSource.class);
            sourceLogInterceptor.setLoggerLevel(UnbufferedLogMinerStreamingChangeEventSource.class, Level.DEBUG);

            final Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9013")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Need to wait for the resume position provider to run once
            Awaitility.await()
                    .atMost(Duration.ofSeconds(30))
                    .until(() -> resumePositionLogInterceptor.containsMessage("Resume/Commit SCN "));

            // Insert a new transaction, which carries the new offset details
            connection.execute("INSERT INTO dbz9013 (id,data) values (1,'test')");

            // Wait until the low watermark is advanced
            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .until(() -> sourceLogInterceptor.containsMessage("Advancing offset low-watermark scn"));

            // Make sure the event is received before stopping the connector
            final SourceRecords records = consumeRecordsByTopic(1);
            assertThat(records.recordsForTopic("server1.DEBEZIUM.DBZ9013")).hasSize(1);

            stopConnector();

            // Read the offsets
            final OraclePartition partition = new OraclePartition(TestHelper.SERVER_NAME, TestHelper.DATABASE);
            final Map<String, Object> committedOffsets = readLastCommittedOffset(config, partition.getSourcePartition());

            // Get SCN passed from the snapshot into the streaming phase
            final Scn snapshotScn = OracleOffsetContext.loadSnapshotScn(committedOffsets);
            assertThat(snapshotScn).isNotNull();

            // Get the SCN low watermark updated by the streaming phase
            final Scn lowWatermarkScn = OracleOffsetContext.getScnFromOffsetMapByKey(committedOffsets, SourceInfo.SCN_KEY);
            assertThat(lowWatermarkScn).isNotNull();

            // Verify the SCN values were updated
            // Before this fix, the lowWatermarkScn was always the same as the snapshotScn
            // With the fix, the low watermark should be after the snapshot scn
            assertThat(lowWatermarkScn.asBigInteger()).isGreaterThan(snapshotScn.asBigInteger());
        }
        finally {
            TestHelper.dropTable(connection, "dbz9013");
        }
    }

}

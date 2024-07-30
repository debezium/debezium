/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.custom.snapshotter.CustomTestSnapshot;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ChangeEventSourceCoordinator;

public class CustomSnapshotterIT extends AbstractAsyncEngineConnectorTest {

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;

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
    @FixFor("DBZ-1082")
    public void shouldAllowForCustomSnapshot() throws InterruptedException {

        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.SNAPSHOT_QUERY_MODE, CommonConnectorConfig.SnapshotQueryMode.CUSTOM)
                .with(PostgresConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        SourceRecords actualRecords = consumeRecordsByTopic(1);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs).isNull();

        SourceRecord record = s1recs.get(0);
        VerifyRecord.isValidRead(record, PK_FIELD, 1);

        TestHelper.execute(INSERT_STMT);
        actualRecords = consumeRecordsByTopic(2);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        record = s1recs.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);
        record = s2recs.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);
        stopConnector();

        config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.CUSTOM.getValue())
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.SNAPSHOT_QUERY_MODE, CommonConnectorConfig.SnapshotQueryMode.CUSTOM)
                .with(PostgresConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        actualRecords = consumeRecordsByTopic(4);

        s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(2);
        assertThat(s2recs.size()).isEqualTo(2);
        VerifyRecord.isValidRead(s1recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s1recs.get(1), PK_FIELD, 2);
        VerifyRecord.isValidRead(s2recs.get(0), PK_FIELD, 1);
        VerifyRecord.isValidRead(s2recs.get(1), PK_FIELD, 2);
    }

    @Test
    public void shouldAllowStreamOnlyByConfigurationBasedSnapshot() throws InterruptedException {

        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.CONFIGURATION_BASED)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_DATA, false)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_SCHEMA, false)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_START_STREAM, true)
                .with(PostgresConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        assertNoRecordsToConsume();

        TestHelper.execute(INSERT_STMT);
        SourceRecords actualRecords = consumeRecordsByTopic(2);

        List<SourceRecord> s1recs = actualRecords.recordsForTopic(topicName("s1.a"));
        List<SourceRecord> s2recs = actualRecords.recordsForTopic(topicName("s2.a"));
        assertThat(s1recs.size()).isEqualTo(1);
        assertThat(s2recs.size()).isEqualTo(1);
        SourceRecord record = s1recs.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);
        record = s2recs.get(0);
        VerifyRecord.isValidInsert(record, PK_FIELD, 2);
        stopConnector();
    }

    @Test
    public void shouldNotAllowStreamByConfigurationBasedSnapshot() {

        LogInterceptor logInterceptor = new LogInterceptor(ChangeEventSourceCoordinator.class);

        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.CONFIGURATION_BASED)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_DATA, false)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_SNAPSHOT_SCHEMA, false)
                .with(CommonConnectorConfig.SNAPSHOT_MODE_CONFIGURATION_BASED_START_STREAM, false)
                .with(PostgresConnectorConfig.SNAPSHOT_QUERY_MODE_CUSTOM_NAME, CustomTestSnapshot.class.getName())
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        assertNoRecordsToConsume();

        TestHelper.execute(INSERT_STMT);

        assertNoRecordsToConsume();

        waitForConnectorShutdown("postgres", TestHelper.TEST_SERVER);

        assertThat(logInterceptor.containsMessage("Streaming is disabled for snapshot mode configuration_based")).isTrue();

        stopConnector();
    }
}

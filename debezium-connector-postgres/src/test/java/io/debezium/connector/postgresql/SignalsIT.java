/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.Signal;

public class SignalsIT extends AbstractConnectorTest {

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);";
    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s1.debezium_signal (id varchar(32), type varchar(32), data varchar(2048));" +
            INSERT_STMT;

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Test
    public void signalLog() throws InterruptedException {
        // Testing.Print.enable();
        final LogInterceptor logInterceptor = new LogInterceptor(Signal.class);

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SIGNAL_DATA_COLLECTION, "s1.debezium_signal")
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForDefaultReplicationSlotBeActive();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        // insert and verify a new record
        TestHelper.execute(INSERT_STMT);

        // Insert the signal record
        TestHelper.execute("INSERT INTO s1.debezium_signal VALUES('1', 'log', '{\"message\": \"Signal message at offset ''{}''\"}')");

        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.allRecordsInOrder()).hasSize(2);
        assertThat(logInterceptor.containsMessage("Received signal")).isTrue();
    }

    @Test
    public void signalingDisabled() throws InterruptedException {
        // Testing.Print.enable();
        final LogInterceptor logInterceptor = new LogInterceptor(Signal.class);

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForDefaultReplicationSlotBeActive();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        // Insert the signal record
        TestHelper.execute("INSERT INTO s1.debezium_signal VALUES('1', 'log', '{\"message\": \"Signal message\"}')");

        // insert and verify a new record
        TestHelper.execute(INSERT_STMT);

        final SourceRecords records = consumeRecordsByTopic(2);
        assertThat(records.allRecordsInOrder()).hasSize(2);
        assertThat(logInterceptor.containsMessage("Received signal")).isFalse();
    }

    @Test
    public void signalSchemaChange() throws InterruptedException {
        // Testing.Print.enable();

        TestHelper.dropDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NEVER.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.TRUE)
                .with(PostgresConnectorConfig.SIGNAL_DATA_COLLECTION, "s1.debezium_signal")
                .build();
        start(PostgresConnector.class, config);
        assertConnectorIsRunning();
        TestHelper.waitForDefaultReplicationSlotBeActive();

        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);
        // there shouldn't be any snapshot records
        assertNoRecordsToConsume();

        // insert and verify a new record
        TestHelper.execute(INSERT_STMT);

        // Insert the signal record - add 'aa' column to PK fields
        TestHelper.execute("INSERT INTO s1.debezium_signal VALUES('1', 'schema-changes', '{\"database\": \"postgres\", \"changes\": [{\n"
                + "  \"type\" : \"ALTER\",\n"
                + "  \"id\" : \"\\\"s1\\\".\\\"a\\\"\",\n"
                + "  \"table\" : {\n"
                + "    \"defaultCharsetName\" : null,\n"
                + "    \"primaryKeyColumnNames\" : [ \"pk\", \"aa\" ],\n"
                + "    \"columns\" : [ {\n"
                + "      \"name\" : \"pk\",\n"
                + "      \"jdbcType\" : 4,\n"
                + "      \"nativeType\" : 23,\n"
                + "      \"typeName\" : \"serial\",\n"
                + "      \"typeExpression\" : \"serial\",\n"
                + "      \"charsetName\" : null,\n"
                + "      \"length\" : 10,\n"
                + "      \"scale\" : 0,\n"
                + "      \"position\" : 1,\n"
                + "      \"optional\" : false,\n"
                + "      \"autoIncremented\" : true,\n"
                + "      \"generated\" : false\n"
                + "    }, {\n"
                + "      \"name\" : \"aa\",\n"
                + "      \"jdbcType\" : 4,\n"
                + "      \"nativeType\" : 23,\n"
                + "      \"typeName\" : \"int4\",\n"
                + "      \"typeExpression\" : \"int4\",\n"
                + "      \"charsetName\" : null,\n"
                + "      \"length\" : 10,\n"
                + "      \"scale\" : 0,\n"
                + "      \"position\" : 2,\n"
                + "      \"optional\" : true,\n"
                + "      \"autoIncremented\" : false,\n"
                + "      \"generated\" : false\n"
                + "    } ]\n"
                + "  }\n"
                + "}]}')");

        TestHelper.execute(INSERT_STMT);

        final SourceRecords records = consumeRecordsByTopic(3);
        assertThat(records.allRecordsInOrder()).hasSize(3);

        final SourceRecord pre = records.allRecordsInOrder().get(0);
        final SourceRecord post = records.allRecordsInOrder().get(2);

        assertThat(((Struct) pre.key()).schema().fields()).hasSize(1);

        final Struct postKey = (Struct) post.key();
        assertThat(postKey.schema().fields()).hasSize(2);
        assertThat(postKey.schema().field("pk")).isNotNull();
        assertThat(postKey.schema().field("aa")).isNotNull();
    }
}

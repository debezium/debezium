/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.SchemaAndValueField;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration test for {@link io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_ISOLATION_MODE}
 */
public class SnapshotIsolationIT extends AbstractAsyncEngineConnectorTest {

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (2);" +
            "INSERT INTO s1.a (aa) VALUES (3);" +
            "INSERT INTO s2.a (aa) VALUES (4);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL NOT NULL PRIMARY KEY, aa integer);" +
            "CREATE TABLE s2.a (pk SERIAL NOT NULL PRIMARY KEY, aa integer);";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;

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
    public void takeSnapshotInSerializableMode() throws Exception {
        takeSnapshot(PostgresConnectorConfig.SnapshotIsolationMode.SERIALIZABLE);
    }

    @Test
    public void takeSnapshotInRepeatableReadMode() throws Exception {
        takeSnapshot(PostgresConnectorConfig.SnapshotIsolationMode.REPEATABLE_READ);
    }

    @Test
    public void takeSnapshotInReadCommittedMode() throws Exception {
        takeSnapshot(PostgresConnectorConfig.SnapshotIsolationMode.READ_COMMITTED);
    }

    @Test
    public void takeSnapshotInReadUncommittedMode() throws Exception {
        takeSnapshot(PostgresConnectorConfig.SnapshotIsolationMode.READ_UNCOMMITTED);
    }

    private void takeSnapshot(PostgresConnectorConfig.SnapshotIsolationMode lockingMode) throws Exception {
        TestHelper.execute(SETUP_TABLES_STMT);

        final Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_ISOLATION_MODE.name(), lockingMode.getValue())
                .build();

        start(PostgresConnector.class, config);
        assertConnectorIsRunning();

        final SourceRecords records = consumeRecordsByTopic(4);

        final List<SourceRecord> table1 = records.recordsForTopic(topicName("s1.a"));
        assertThat(table1).hasSize(2);

        final List<SourceRecord> table2 = records.recordsForTopic(topicName("s2.a"));
        assertThat(table2).hasSize(2);

        final List<SourceRecord> tables = List.of(
                table1.get(0),
                table2.get(0),
                table1.get(1),
                table2.get(1));

        final Schema pkSchema = SchemaBuilder.int32().defaultValue(0).build();

        for (int i = 0; i < 4; i++) {
            final SourceRecord record1 = tables.get(i);

            final List<SchemaAndValueField> expectedKey1 = List.of(
                    new SchemaAndValueField("pk", pkSchema, i / 2 + 1));
            final List<SchemaAndValueField> expectedRow1 = List.of(
                    new SchemaAndValueField("pk", pkSchema, i / 2 + 1),
                    new SchemaAndValueField("aa", Schema.OPTIONAL_INT32_SCHEMA, i + 1));

            final Struct key1 = (Struct) record1.key();
            final Struct value1 = (Struct) record1.value();
            assertRecord(key1, expectedKey1);
            assertRecord((Struct) value1.get("after"), expectedRow1);
            assertThat(record1.sourceOffset()).extracting("snapshot").isEqualTo("INITIAL");
            assertThat(record1.sourceOffset()).extracting("last_snapshot_record").isEqualTo(i == 4 - 1);
            assertNull(value1.get("before"));
        }
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

/**
 * Integration test to verify PgVector types.
 *
 * @author Jiri Pechanec
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 15, reason = "PgVector is tested only with PostgreSQL 15+")
public class VectorDatabaseIT extends AbstractRecordsProducerTest {

    @Rule
    public final SkipTestRule skipTest = new SkipTestRule();

    @Before
    public void before() throws Exception {
        // ensure the slot is deleted for each test
        try (PostgresConnection conn = TestHelper.create()) {
            conn.dropReplicationSlot(ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        }
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("init_pgvector.ddl");
        TestHelper.execute(
                "CREATE TABLE pgvector.table_vector (pk SERIAL, f_vector pgvector.vector(3), f_halfvec pgvector.halfvec(3), f_sparsevec pgvector.sparsevec(3000), PRIMARY KEY(pk));",
                "INSERT INTO pgvector.table_vector (f_vector, f_halfvec, f_sparsevec) VALUES ('[1,2,3]', '[101,102,103]', '{1: 201, 9: 209}/3000');");
        initializeConnectorTestFramework();
    }

    @Test
    public void shouldSnapshotAndStreamData() throws Exception {
        Testing.Print.enable();

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);

        TestHelper.execute("INSERT INTO pgvector.table_vector (f_vector, f_halfvec, f_sparsevec) VALUES ('[10,20,30]', '[110,120,130]', '{1: 301, 9: 309}/3000');");

        var actualRecords = consumeRecordsByTopic(2);
        var recs = actualRecords.recordsForTopic("test_server.pgvector.table_vector");
        Assertions.assertThat(recs).hasSize(2);

        var rec = ((Struct) recs.get(0).value());
        Assertions.assertThat(rec.schema().field("after").schema().field("f_vector").schema().name()).isEqualTo("io.debezium.data.DoubleVector");
        Assertions.assertThat(rec.schema().field("after").schema().field("f_halfvec").schema().name()).isEqualTo("io.debezium.data.FloatVector");
        Assertions.assertThat(rec.schema().field("after").schema().field("f_sparsevec").schema().name()).isEqualTo("io.debezium.data.SparseVector");
        Assertions.assertThat(rec.getStruct("after").getArray("f_vector")).isEqualTo(List.of(1.0, 2.0, 3.0));
        Assertions.assertThat(rec.getStruct("after").getArray("f_halfvec")).isEqualTo(List.of(101.0f, 102.0f, 103.0f));
        Assertions.assertThat(rec.getStruct("after").getStruct("f_sparsevec").getInt16("dimensions")).isEqualTo((short) 3000);
        Assertions.assertThat(rec.getStruct("after").getStruct("f_sparsevec").getMap("vector")).isEqualTo(Map.of((short) 1, 201.0, (short) 9, 209.0));

        rec = ((Struct) recs.get(1).value());
        Assertions.assertThat(rec.schema().field("after").schema().field("f_vector").schema().name()).isEqualTo("io.debezium.data.DoubleVector");
        Assertions.assertThat(rec.schema().field("after").schema().field("f_halfvec").schema().name()).isEqualTo("io.debezium.data.FloatVector");
        Assertions.assertThat(rec.schema().field("after").schema().field("f_sparsevec").schema().name()).isEqualTo("io.debezium.data.SparseVector");
        Assertions.assertThat(rec.getStruct("after").getArray("f_vector")).isEqualTo(List.of(10.0, 20.0, 30.0));
        Assertions.assertThat(rec.getStruct("after").getArray("f_halfvec")).isEqualTo(List.of(110.0f, 120.0f, 130.0f));
        Assertions.assertThat(rec.getStruct("after").getStruct("f_sparsevec").getInt16("dimensions")).isEqualTo((short) 3000);
        Assertions.assertThat(rec.getStruct("after").getStruct("f_sparsevec").getMap("vector")).isEqualTo(Map.of((short) 1, 301.0, (short) 9, 309.0));
    }

    @Test
    public void shouldStreamData() throws Exception {
        Testing.Print.enable();
        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build());
        assertConnectorIsRunning();

        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        TestHelper.execute(
                "DROP TABLE IF EXISTS pgvector.table_vector_str;",
                "CREATE TABLE pgvector.table_vector_str (pk SERIAL, f_vector pgvector.vector(3), f_halfvec pgvector.halfvec(3), f_sparsevec pgvector.sparsevec(3000), PRIMARY KEY(pk));",
                "INSERT INTO pgvector.table_vector_str (f_vector, f_halfvec, f_sparsevec) VALUES ('[1,2,3]', '[101,102,103]', '{1: 201, 9: 209}/3000');");

        var actualRecords = consumeRecordsByTopic(1);
        var recs = actualRecords.recordsForTopic("test_server.pgvector.table_vector_str");
        Assertions.assertThat(recs).hasSize(1);

        var rec = ((Struct) recs.get(0).value());
        Assertions.assertThat(rec.schema().field("after").schema().field("f_vector").schema().name()).isEqualTo("io.debezium.data.DoubleVector");
        Assertions.assertThat(rec.getStruct("after").getArray("f_vector")).isEqualTo(List.of(1.0, 2.0, 3.0));
    }
}

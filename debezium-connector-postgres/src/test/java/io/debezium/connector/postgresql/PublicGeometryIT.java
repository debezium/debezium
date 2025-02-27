/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDecoderPluginNameRule;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.junit.ConditionalFail;
import io.debezium.relational.TableId;

/**
 * Integration test to verify PostGIS types defined in public schema.
 *
 * @author Jiri Pechanec
 */
public class PublicGeometryIT extends AbstractRecordsProducerTest {

    private TestConsumer consumer;

    @Rule
    public final TestRule skip = new SkipTestDependingOnDecoderPluginNameRule();

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    @Before
    public void before() throws Exception {
        // ensure the slot is deleted for each test
        try (PostgresConnection conn = TestHelper.create()) {
            conn.dropReplicationSlot(ReplicationConnection.Builder.DEFAULT_SLOT_NAME);
        }
        TestHelper.dropAllSchemas();
        TestHelper.execute(
                "DROP SCHEMA IF EXISTS postgis CASCADE;",
                "CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;",
                "CREATE TABLE public.postgis_table (pk SERIAL, p GEOMETRY(POINT,3187), ml GEOGRAPHY(MULTILINESTRING), PRIMARY KEY(pk));",
                "CREATE TABLE public.postgis_array_table (pk SERIAL, ga GEOMETRY[], gann GEOMETRY[] NOT NULL, PRIMARY KEY(pk));",
                "CREATE TABLE public.dummy_table (pk SERIAL, PRIMARY KEY(pk));");
        setupRecordsProducer(TestHelper.defaultConfig());
    }

    @Test(timeout = 30000)
    @FixFor("DBZ-1144")
    public void shouldReceiveChangesForInsertsWithPostgisTypes() throws Exception {
        consumer = testConsumer(1, "public");
        waitForStreamingToStart();
        // need to wait for all the spatial_ref_sys to flow through and be ignored.
        // this exceeds the normal 2s timeout.
        TestHelper.execute("INSERT INTO public.dummy_table DEFAULT VALUES;");
        consumer.await(TestHelper.waitTimeForRecords() * 10, TimeUnit.SECONDS);
        while (true) {
            if (!consumer.isEmpty()) {
                SourceRecord record = consumer.remove();
                if (record.topic().endsWith(".public.dummy_table")) {
                    break;
                }
            }
        }

        // now do it for actual testing
        // postgis types
        consumer.expects(1);
        assertInsert(INSERT_POSTGIS_TYPES_IN_PUBLIC_STMT, 1, schemaAndValuesForPostgisTypes());
        consumer.expects(1);
        assertInsert(INSERT_POSTGIS_ARRAY_TYPES_IN_PUBLIC_STMT, 1, schemaAndValuesForPostgisArrayTypes());
    }

    private void setupRecordsProducer(Configuration.Builder config) {
        start(PostgresConnector.class, config
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .build());
        assertConnectorIsRunning();
    }

    private void assertInsert(String statement, Integer pk, List<SchemaAndValueField> expectedSchemaAndValuesByColumn) {
        TableId table = tableIdFromInsertStmt(statement);
        String expectedTopicName = table.schema() + "." + table.table();
        expectedTopicName = expectedTopicName.replaceAll("[ \"]", "_");

        try {
            executeAndWait(statement);
            SourceRecord record = assertRecordInserted(expectedTopicName, pk != null ? PK_FIELD : null, pk);
            assertRecordOffsetAndSnapshotSource(record, SnapshotRecord.FALSE);
            assertSourceInfo(record, "postgres", table.schema(), table.table());
            assertRecordSchemaAndValues(expectedSchemaAndValuesByColumn, record, Envelope.FieldName.AFTER);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SourceRecord assertRecordInserted(String expectedTopicName, String pkColumn, Integer pk) throws InterruptedException {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord insertedRecord = consumer.remove();
        assertEquals(topicName(expectedTopicName), insertedRecord.topic());

        if (pk != null) {
            VerifyRecord.isValidInsert(insertedRecord, pkColumn, pk);
        }
        else {
            VerifyRecord.isValidInsert(insertedRecord);
        }

        return insertedRecord;
    }

    private void executeAndWait(String statements) throws Exception {
        TestHelper.execute(statements);
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);
    }
}

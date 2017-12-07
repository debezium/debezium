/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static io.debezium.connector.postgresql.TestHelper.topicName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.debezium.doc.FixFor;
import io.debezium.jdbc.TemporalPrecisionMode;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;

/**
 * Integration test for {@link RecordsSnapshotProducerIT}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class RecordsSnapshotProducerIT extends AbstractRecordsProducerTest {

    private RecordsSnapshotProducer snapshotProducer;
    private PostgresTaskContext context;

    @Before
    public void before() throws SQLException {
        TestHelper.dropAllSchemas();

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        context = new PostgresTaskContext(config, new PostgresSchema(config));
    }

    @After
    public void after() throws Exception {
        if (snapshotProducer != null) {
            snapshotProducer.stop();
        }
    }

    @Test
    public void shouldGenerateSnapshotsForDefaultDatatypes() throws Exception {
        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER), false);

        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestConsumer consumer = testConsumer(ALL_STMTS.size());

        //insert data for each of different supported types
        String statementsBuilder = ALL_STMTS.stream().collect(Collectors.joining(";" + System.lineSeparator())) + ";";
        TestHelper.execute(statementsBuilder);

        //then start the producer and validate all records are there
        snapshotProducer.start(consumer);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        Map<String, List<SchemaAndValueField>> expectedValuesByTableName = super.schemaAndValuesByTableName();
        consumer.process(record -> assertReadRecord(record, expectedValuesByTableName));

        // check the offset information for each record
        while (!consumer.isEmpty()) {
            SourceRecord record = consumer.remove();
            assertRecordOffset(record, true, consumer.isEmpty());
        }
    }

    @Test
    public void shouldGenerateSnapshotAndContinueStreaming() throws Exception {
        String insertStmt = "INSERT INTO s1.a (aa) VALUES (1);" +
                            "INSERT INTO s2.a (aa) VALUES (1);";

        String statements = "CREATE SCHEMA s1; " +
                            "CREATE SCHEMA s2; " +
                            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                            "CREATE TABLE s2.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
                            insertStmt;
        TestHelper.execute(statements);

        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER), true);
        TestConsumer consumer = testConsumer(2, "s1", "s2");
        snapshotProducer.start(consumer);

        // first make sure we get the initial records from both schemas...
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        consumer.clear();

        // then insert some more data and check that we get it back
        TestHelper.execute(insertStmt);
        consumer.expects(2);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        SourceRecord first = consumer.remove();
        VerifyRecord.isValidInsert(first, PK_FIELD, 2);
        assertEquals(topicName("s1.a"), first.topic());
        assertRecordOffset(first, false, false);

        SourceRecord second = consumer.remove();
        VerifyRecord.isValidInsert(second, PK_FIELD, 2);
        assertEquals(topicName("s2.a"), second.topic());
        assertRecordOffset(second, false, false);

        // now shut down the producers and insert some more records
        snapshotProducer.stop();
        TestHelper.execute(insertStmt);

        // start a new producer back up, take a new snapshot (we expect all the records to be read back)
        int expectedRecordsCount = 6;
        consumer = testConsumer(expectedRecordsCount, "s1", "s2");
        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER), true);
        snapshotProducer.start(consumer);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        AtomicInteger counter = new AtomicInteger(0);
        consumer.process(record -> {
            int counterVal = counter.getAndIncrement();
            int expectedPk = (counterVal % 3) + 1; //each table has 3 entries keyed 1-3
            VerifyRecord.isValidRead(record, PK_FIELD, expectedPk);
            assertRecordOffset(record, true, counterVal == (expectedRecordsCount - 1));
        });
        consumer.clear();

        // now insert two more records and check that we only get those back from the stream
        TestHelper.execute(insertStmt);
        consumer.expects(2);

        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        first = consumer.remove();
        VerifyRecord.isValidInsert(first, PK_FIELD, 4);
        assertRecordOffset(first, false, false);

        second = consumer.remove();
        VerifyRecord.isValidInsert(second, PK_FIELD, 4);
        assertRecordOffset(second, false, false);
    }

    private void assertReadRecord(SourceRecord record, Map<String, List<SchemaAndValueField>> expectedValuesByTableName) {
        VerifyRecord.isValidRead(record, PK_FIELD, 1);
        String tableName = record.topic().replace(TestHelper.TEST_SERVER + ".", "");
        List<SchemaAndValueField> expectedValuesAndSchemasForTable = expectedValuesByTableName.get(tableName);
        assertNotNull("No expected values for " + tableName + " found", expectedValuesAndSchemasForTable);
        assertRecordSchemaAndValues(expectedValuesAndSchemasForTable, record, Envelope.FieldName.AFTER);
    }

    @Test
    @FixFor("DBZ-342")
    public void shouldGenerateSnapshotsForDefaultDatatypesAdpativeMicroseconds() throws Exception {
        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                        .with(PostgresConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS)
                        .build());
        context = new PostgresTaskContext(config, new PostgresSchema(config));

        snapshotProducer = new RecordsSnapshotProducer(context, new SourceInfo(TestHelper.TEST_SERVER), false);

        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestConsumer consumer = testConsumer(ALL_STMTS.size());

        //insert data for each of different supported types
        String statementsBuilder = ALL_STMTS.stream().collect(Collectors.joining(";" + System.lineSeparator())) + ";";
        TestHelper.execute(statementsBuilder);

        //then start the producer and validate all records are there
        snapshotProducer.start(consumer);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        Map<String, List<SchemaAndValueField>> expectedValuesByTableName = super.schemaAndValuesByTableNameAdaptiveTimeMicroseconds();
        consumer.process(record -> assertReadRecord(record, expectedValuesByTableName));

        // check the offset information for each record
        while (!consumer.isEmpty()) {
            SourceRecord record = consumer.remove();
            assertRecordOffset(record, true, consumer.isEmpty());
        }
    }
}

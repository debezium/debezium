/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.transforms.AbstractVectorToJsonConverterTest;

/**
 * Performs tests of the {@link io.debezium.transforms.VectorToJsonConverter} for PostgreSQL sources.
 *
 * @author Chris Cranford
 */
public class PostgresVectorToJsonConverterIT extends AbstractVectorToJsonConverterTest<PostgresConnector> {

    @Override
    protected Class<PostgresConnector> getConnectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return TestHelper.create();
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1\\.dbz8571");
    }

    @Override
    protected String topicName() {
        return TestHelper.TEST_SERVER + ".s1.dbz8571";
    }

    @Override
    protected void doBefore() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.execute("DROP SCHEMA IF EXISTS s1 CASCADE;CREATE SCHEMA s1;");
        TestHelper.execute("CREATE EXTENSION IF NOT EXISTS vector;");
    }

    @Override
    protected void doAfter() throws Exception {
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
    }

    @Override
    protected void createFloatVectorTable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8571 (id int primary key, data halfvec(3));");
        TestHelper.execute("ALTER TABLE s1.dbz8571 REPLICA IDENTITY FULL;");
    }

    @Override
    protected void createFloatVectorSnapshotData() throws Exception {
        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (1,'[101,102,103]');");
    }

    @Override
    protected void createFloatVectorStreamData() throws Exception {
        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (2,'[1,2,3]');");
        TestHelper.execute("UPDATE s1.dbz8571 set data = '[5,7,9]' WHERE id = 2;");
        TestHelper.execute("DELETE FROM s1.dbz8571 WHERE id = 2");
    }

    @Override
    protected void createDoubleVectorTable() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8571 (id int primary key, data vector(3));");
        TestHelper.execute("ALTER TABLE s1.dbz8571 REPLICA IDENTITY FULL;");
    }

    @Override
    protected void createDoubleVectorSnapshotData() throws Exception {
        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (1,'[101,102,103]');");
    }

    @Override
    protected void createDoubleVectorStreamData() throws Exception {
        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (2,'[1,2,3]');");
        TestHelper.execute("UPDATE s1.dbz8571 set data = '[5,7,9]' WHERE id = 2;");
        TestHelper.execute("DELETE FROM s1.dbz8571 WHERE id = 2");
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
    }

    @Test
    @FixFor("DBZ-8571")
    public void shouldConvertSparseVectorToJson() throws Exception {
        TestHelper.execute("CREATE TABLE s1.dbz8571 (id int primary key, data sparsevec(25));");
        TestHelper.execute("ALTER TABLE s1.dbz8571 REPLICA IDENTITY FULL;");
        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (1,'{1: 25, 2: 15, 10: 100}/25');");

        start(getConnectorClass(), getConfigurationWithTransform());
        assertConnectorIsRunning();

        waitForStreamingStarted();

        TestHelper.execute("INSERT INTO s1.dbz8571 (id,data) values (2,'{2: 10, 5: 20, 20: 30}/25');");
        TestHelper.execute("UPDATE s1.dbz8571 set data = '{1:5,2:10,3:25}/25' WHERE id = 2;");
        TestHelper.execute("DELETE FROM s1.dbz8571 WHERE id = 2");

        final SourceRecords records = consumeRecordsByTopic(5);
        final List<SourceRecord> tableRecords = records.recordsForTopic(topicName());
        assertThat(tableRecords).hasSize(5);

        assertRead(tableRecords.get(0), 1, "{ \"dimensions\": 25, \"vector\": { \"1\": 25.0, \"2\": 15.0, \"10\": 100.0 } }");
        assertInsert(tableRecords.get(1), 2, "{ \"dimensions\": 25, \"vector\": { \"20\": 30.0, \"5\": 20.0, \"2\": 10.0 } }");
        assertUpdate(tableRecords.get(2), 2, "{ \"dimensions\": 25, \"vector\": { \"20\": 30.0, \"5\": 20.0, \"2\": 10.0 } }",
                "{ \"dimensions\": 25, \"vector\": { \"1\": 5.0, \"2\": 10.0, \"3\": 25.0 } }");
        assertDelete(tableRecords.get(3), 2, "{ \"dimensions\": 25, \"vector\": { \"1\": 5.0, \"2\": 10.0, \"3\": 25.0 } }");
        assertTombstone(tableRecords.get(4), 2);
    }

}

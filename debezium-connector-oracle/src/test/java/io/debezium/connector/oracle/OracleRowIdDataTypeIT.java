/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIs;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIs(value = SkipWhenAdapterNameIs.AdapterName.XSTREAM, reason = "XStream does not support ROWID data types")
public class OracleRowIdDataTypeIT extends AbstractConnectorTest {

    private static OracleConnection connection;

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @BeforeClass
    public static void beforeClass() throws Exception {
        connection = TestHelper.testConnection();

        TestHelper.dropTable(connection, "debezium.type_rowid");

        String ddl = "create table debezium.type_rowid (" +
                "  id numeric(9,0) not null, " +
                "  name varchar2(50), " +
                "  parent_rowid rowid, " +
                "  primary key (id)" +
                ")";

        connection.execute(ddl);
        TestHelper.streamTable(connection, "debezium.type_rowid");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (connection != null) {
            TestHelper.dropTable(connection, "debezium.type_rowid");
            connection.close();
        }
    }

    @Before
    public void before() throws Exception {
        connection.execute("delete from debezium.type_rowid");
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @Test
    @FixFor("DBZ-4595")
    public void shouldSnapshotAndStreamRowIdAndURowIdColumnTypes() throws Exception {
        // Insert parent and child records
        connection.executeWithoutCommitting("INSERT INTO debezium.type_rowid (id,name) values (1,'Parent')");
        final String parentRowId = connection.queryAndMap("SELECT ROWID FROM debezium.type_rowid WHERE id = 1", rs -> {
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        });
        connection.executeWithoutCommitting("INSERT INTO debezium.type_rowid (id,name,parent_rowid) " +
                "values (2,'Child','" + parentRowId + "')");
        connection.commit();

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.TYPE_ROWID")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        // Test snapshot
        SourceRecords snapshot = consumeRecordsByTopic(2);
        assertThat(snapshot.allRecordsInOrder()).hasSize(2);

        List<SourceRecord> records = snapshot.recordsForTopic("server1.DEBEZIUM.TYPE_ROWID");
        assertThat(records).hasSize(2);

        Struct after = ((Struct) records.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(1);
        assertThat(after.get("NAME")).isEqualTo("Parent");
        assertThat(after.get("PARENT_ROWID")).isNull();

        after = ((Struct) records.get(1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Child");
        assertThat(after.get("PARENT_ROWID")).isEqualTo(parentRowId);

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
        connection.execute("UPDATE debezium.type_rowid set name = 'Only Child' WHERE id = 2");

        // Test streaming
        SourceRecords stream = consumeRecordsByTopic(1);
        assertThat(stream.allRecordsInOrder()).hasSize(1);

        records = stream.recordsForTopic("server1.DEBEZIUM.TYPE_ROWID");
        assertThat(records).hasSize(1);

        after = ((Struct) records.get(0).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.get("ID")).isEqualTo(2);
        assertThat(after.get("NAME")).isEqualTo("Only Child");
        assertThat(after.get("PARENT_ROWID")).isEqualTo(parentRowId);
    }
}

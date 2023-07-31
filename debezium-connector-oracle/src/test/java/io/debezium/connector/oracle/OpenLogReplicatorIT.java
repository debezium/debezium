/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * @author Chris Cranford
 */
public class OpenLogReplicatorIT extends AbstractConnectorTest {

    private OracleConnection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        stopConnector();
        if (connection != null && connection.isConnected()) {
            connection.close();
        }
    }

    @Test
    public void shouldStreamChanges() throws Exception {
        TestHelper.dropTable(connection, "olr_test");
        try {
            connection.execute("CREATE TABLE olr_test (id number(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "olr_test");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, "schema_only")
                    .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "olr")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO olr_test values (1,'test')");
            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.OLR_TEST");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);
            assertThat(((Struct) tableRecords.get(0).value()).getStruct("after").get("DATA")).isEqualTo("test");

            connection.execute("UPDATE olr_test SET data = 'updated' where id = 1");
            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.OLR_TEST");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidUpdate(tableRecords.get(0), "ID", 1);
            assertThat(((Struct) tableRecords.get(0).value()).getStruct("after").get("DATA")).isEqualTo("updated");

            connection.execute("delete from olr_test");
            records = consumeRecordsByTopic(1);
            tableRecords = records.recordsForTopic("server1.DEBEZIUM.OLR_TEST");
            assertThat(tableRecords).hasSize(1);
            VerifyRecord.isValidDelete(tableRecords.get(0), "ID", 1);
            assertThat(((Struct) tableRecords.get(0).value()).getStruct("before").get("DATA")).isEqualTo("updated");
        }
        finally {
            TestHelper.dropTable(connection, "olr_test");
        }
    }

}

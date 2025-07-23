/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName.ANY_LOGMINER;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Tests using LogMiner with the internal CTE-based query functionality.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = ANY_LOGMINER)
public class CteQueryIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-9272")
    public void shouldStreamChangesUsingCteBasedQueries() throws Exception {
        TestHelper.dropTable(connection, "dbz9272");
        try {
            connection.execute("CREATE TABLE dbz9272 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz9272");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ9272")
                    .with(OracleConnectorConfig.LOG_MINING_USE_CTE_QUERY, "true")
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "in")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz9272 values (1, 'insert')");
            connection.execute("UPDATE dbz9272 SET data = 'update' where ID = 1");
            connection.execute("DELETE FROM dbz9272 WHERE id = 1");

            List<SourceRecord> records = consumeRecordsByTopic(4).recordsForTopic("server1.DEBEZIUM.DBZ9272");
            assertThat(records).hasSize(4);

            Struct value = ((Struct) records.get(0).value());
            VerifyRecord.isValidInsert(records.get(0), "ID", 1);
            assertThat(value.getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(1);
            assertThat(value.getStruct(Envelope.FieldName.AFTER).get("DATA")).isEqualTo("insert");

            value = ((Struct) records.get(1).value());
            VerifyRecord.isValidUpdate(records.get(1), "ID", 1);
            assertThat(value.getStruct(Envelope.FieldName.BEFORE).get("ID")).isEqualTo(1);
            assertThat(value.getStruct(Envelope.FieldName.BEFORE).get("DATA")).isEqualTo("insert");
            assertThat(value.getStruct(Envelope.FieldName.AFTER).get("ID")).isEqualTo(1);
            assertThat(value.getStruct(Envelope.FieldName.AFTER).get("DATA")).isEqualTo("update");

            value = ((Struct) records.get(2).value());
            VerifyRecord.isValidDelete(records.get(2), "ID", 1);
            assertThat(value.getStruct(Envelope.FieldName.BEFORE).get("ID")).isEqualTo(1);
            assertThat(value.getStruct(Envelope.FieldName.BEFORE).get("DATA")).isEqualTo("update");

            VerifyRecord.isValidTombstone(records.get(3));
        }
        finally {
            TestHelper.dropTable(connection, "dbz9272");
        }
    }
}

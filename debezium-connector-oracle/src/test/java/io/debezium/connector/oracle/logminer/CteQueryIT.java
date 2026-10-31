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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Tests that the removed internal CTE-based query option no longer affects LogMiner streaming.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = ANY_LOGMINER)
public class CteQueryIT extends AbstractAsyncEngineConnectorTest {

    private static OracleConnection connection;

    @BeforeAll
    static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
    }

    @AfterAll
    static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("debezium/dbz#2984")
    @SuppressWarnings("deprecation")
    public void shouldWarnAndStreamChangesWhenRemovedCteQueryOptionIsEnabled() throws Exception {
        TestHelper.dropTable(connection, "dbz2984");
        try {
            connection.execute("CREATE TABLE dbz2984 (id numeric(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz2984");

            // Before the CTE query mode was removed, this combination failed validation as the
            // CTE query required a query filter mode other than 'none'.
            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ2984")
                    .with(OracleConnectorConfig.LOG_MINING_USE_CTE_QUERY, "true")
                    .with(OracleConnectorConfig.LOG_MINING_QUERY_FILTER_MODE, "none")
                    .build();

            final LogInterceptor logInterceptor = new LogInterceptor(OracleConnectorConfig.class);

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            assertThat(logInterceptor.containsWarnMessage("is no longer supported, the CTE query mode has been removed")).isTrue();

            connection.execute("INSERT INTO dbz2984 values (1, 'insert')");
            connection.execute("UPDATE dbz2984 SET data = 'update' where ID = 1");
            connection.execute("DELETE FROM dbz2984 WHERE id = 1");

            List<SourceRecord> records = consumeRecordsByTopic(4).recordsForTopic("server1.DEBEZIUM.DBZ2984");
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
            TestHelper.dropTable(connection, "dbz2984");
        }
    }
}

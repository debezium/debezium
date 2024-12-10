/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
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
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Tests for LogMiner's source info block REDO SQL
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(value = SkipWhenAdapterNameIsNot.AdapterName.LOGMINER, reason = "Requires LogMiner")
public class LogMinerSourceInfoRedoSqlIT extends AbstractConnectorTest {

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

    @Before
    public void before() throws SQLException {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @Test
    @FixFor("DBZ-6960")
    public void shouldStreamRedoSqlInSourceInfoBlockWhenEnabled() throws Exception {
        testRedoSqlInSourceInfoBlock(true);
    }

    @Test
    @FixFor("DBZ-6960")
    public void shouldNotIncludeRedoSqlInSourceInfoBlockWhenNotEnabled() throws Exception {
        testRedoSqlInSourceInfoBlock(false);
    }

    private void testRedoSqlInSourceInfoBlock(boolean includeRedoSql) throws Exception {
        TestHelper.dropTable(connection, "dbz6960");
        try {
            connection.execute("CREATE TABLE dbz6960 (id number(9,0) primary key, data varchar2(50))");
            TestHelper.streamTable(connection, "dbz6960");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.LOG_MINING_INCLUDE_REDO_SQL, Boolean.toString(includeRedoSql))
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ6960")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            connection.execute("INSERT INTO dbz6960 (id,data) values (1, 'test')");

            SourceRecords records = consumeRecordsByTopic(1);
            List<SourceRecord> tableRecords = records.recordsForTopic("server1.DEBEZIUM.DBZ6960");
            assertThat(tableRecords).hasSize(1);

            VerifyRecord.isValidInsert(tableRecords.get(0), "ID", 1);

            Struct source = ((Struct) tableRecords.get(0).value()).getStruct(Envelope.FieldName.SOURCE);
            assertThat(source.schema().field(SourceInfo.REDO_SQL)).isNotNull();

            String redoSql = source.getString(SourceInfo.REDO_SQL);
            if (includeRedoSql) {
                assertThat(redoSql).isEqualTo("insert into \"DEBEZIUM\".\"DBZ6960\"(\"ID\",\"DATA\") values ('1','test');");
            }
            else {
                assertThat(redoSql).isNull();
            }
        }
        finally {
            TestHelper.dropTable(connection, "dbz6960");
        }
    }
}

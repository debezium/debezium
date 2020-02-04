/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

/**
 * Transaction metadata test for the Debezium DB2 Server connector.
 *
 * @author Jiri Pechanec
 */
public class TransactionMetadataIT extends AbstractConnectorTest {

    private Db2Connection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute(
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))",
                "INSERT INTO tablea VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "TABLEA");
        TestHelper.enableTableCdc(connection, "TABLEB");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "TABLEB");
            TestHelper.disableTableCdc(connection, "TABLEA");
            connection.execute("DROP TABLE tablea", "DROP TABLE tableb");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
            connection.close();
        }
    }

    @Test
    public void transactionMetadata() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'DB2INST1'");
        TestHelper.refreshAndWait(connection);

        connection.setAutoCommit(false);
        final String[] inserts = new String[RECORDS_PER_TABLE * 2];
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            inserts[2 * i] = "INSERT INTO tablea VALUES(" + id + ", 'a')";
            inserts[2 * i + 1] = "INSERT INTO tableb VALUES(" + id + ", 'b')";
        }
        connection.execute(inserts);
        connection.setAutoCommit(true);

        connection.execute("INSERT INTO tableb VALUES(1000, 'b')");

        TestHelper.refreshAndWait(connection);

        // BEGIN, data, END, BEGIN, data
        final SourceRecords records = consumeRecordsByTopic(1 + RECORDS_PER_TABLE * 2 + 1 + 1 + 1);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.DB2INST1.TABLEB");
        final List<SourceRecord> tx = records.recordsForTopic("testdb.transaction");
        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE + 1);
        Assertions.assertThat(tx).hasSize(3);

        final List<SourceRecord> all = records.allRecordsInOrder();
        final String txId = assertBeginTransaction(all.get(0));

        long counter = 1;
        for (int i = 1; i <= 2 * RECORDS_PER_TABLE; i++) {
            assertRecordTransactionMetadata(all.get(i), txId, counter, (counter + 1) / 2);
            counter++;
        }

        assertEndTransaction(all.get(2 * RECORDS_PER_TABLE + 1), txId, 2 * RECORDS_PER_TABLE,
                Collect.hashMapOf("DB2INST1.TABLEA", RECORDS_PER_TABLE, "DB2INST1.TABLEB", RECORDS_PER_TABLE));
        stopConnector();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnector;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Chris Cranford
 */
public class TransactionCommitConsumerIT extends AbstractConnectorTest {

    private static OracleConnection connection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        connection = TestHelper.testConnection();
        TestHelper.dropAllTables();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-6963")
    public void testShouldNotConsolidateEventsWhenTableHasNoLobColumns() throws Exception {
        try {
            connection.execute("CREATE TABLE addresses (ID numeric(9,0) primary key, person_id numeric(9,0))");
            connection.execute("CREATE TABLE email (ID numeric(9,0) primary key, person_id numeric(9,0))");
            connection.execute("CREATE TABLE phone (ID numeric(9,0) primary key, person_id numeric(9,0))");

            // Seed data
            connection.execute("INSERT INTO addresses values (-1,-1)");
            connection.execute("INSERT INTO email values (-1,-1)");
            connection.execute("INSERT INTO phone values (-1,-1)");

            TestHelper.streamTable(connection, "addresses");
            TestHelper.streamTable(connection, "email");
            TestHelper.streamTable(connection, "phone");

            Configuration config = TestHelper.defaultConfig()
                    .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.ADDRESSES,DEBEZIUM\\.EMAIL,DEBEZIUM\\.PHONE")
                    .with(OracleConnectorConfig.LOB_ENABLED, "true")
                    .with(OracleConnectorConfig.SNAPSHOT_MODE, "schema_only")
                    .build();

            start(OracleConnector.class, config);
            assertConnectorIsRunning();

            waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

            // Perform several iterations to create a large transaction with specific sequences of changes against
            // tables that will be viewed by the TransactionCommitConsumer as eligible for merging since LOB is
            // enabled but that shouldn't be because the tables have no LOB columns.
            connection.setAutoCommit(false);
            final int ITERATIONS = 25;
            for (int i = 0; i < ITERATIONS; i++) {
                connection.executeWithoutCommitting("INSERT INTO addresses (ID,PERSON_ID) values (" + i + ",-1)");
                connection.executeWithoutCommitting("UPDATE email SET person_id = " + i + " WHERE id = -1");
                connection.executeWithoutCommitting("INSERT INTO email (ID,PERSON_ID) values (" + i + ",-1)");
                connection.executeWithoutCommitting("UPDATE email SET person_id = " + (i + 999) + " WHERE id = " + i);
                connection.executeWithoutCommitting("UPDATE email SET person_id = " + (i + 1000) + " WHERE id = -1");
                connection.executeWithoutCommitting("UPDATE phone SET person_id = " + i + " WHERE id = -1");
                connection.executeWithoutCommitting("INSERT INTO phone (ID,PERSON_ID) values (" + i + ",-1)");
                connection.executeWithoutCommitting("UPDATE phone SET person_id = " + i + " WHERE id = " + i);
                connection.executeWithoutCommitting("UPDATE phone SET person_id = -1 WHERE id = -1");
                connection.executeWithoutCommitting("UPDATE addresses SET person_id = " + i + " WHERE id = -1");
            }
            connection.commit();

            SourceRecords records = consumeRecordsByTopic(ITERATIONS * 10);

            final List<SourceRecord> addresses = records.recordsForTopic("server1.DEBEZIUM.ADDRESSES");
            assertThat(addresses).hasSize(2 * ITERATIONS);

            for (int i = 0, k = 0; i < addresses.size(); i += 2, k++) {
                VerifyRecord.isValidInsert(addresses.get(i), "ID", k);
                VerifyRecord.isValidUpdate(addresses.get(i + 1), "ID", -1);
            }

            final List<SourceRecord> phones = records.recordsForTopic("server1.DEBEZIUM.PHONE");
            assertThat(phones).hasSize(4 * ITERATIONS);

            for (int i = 0, k = 0; i < phones.size(); i += 4, k++) {
                VerifyRecord.isValidUpdate(phones.get(i), "ID", -1);
                VerifyRecord.isValidInsert(phones.get(i + 1), "ID", k);
                VerifyRecord.isValidUpdate(phones.get(i + 2), "ID", k);
                VerifyRecord.isValidUpdate(phones.get(i + 3), "ID", -1);
            }

            final List<SourceRecord> emails = records.recordsForTopic("server1.DEBEZIUM.EMAIL");
            assertThat(emails).hasSize(4 * ITERATIONS);

            for (int i = 0, k = 0; i < emails.size(); i += 4, k++) {
                VerifyRecord.isValidUpdate(emails.get(i), "ID", -1);
                VerifyRecord.isValidInsert(emails.get(i + 1), "ID", k);
                VerifyRecord.isValidUpdate(emails.get(i + 2), "ID", 0);
                VerifyRecord.isValidUpdate(emails.get(i + 3), "ID", -1);
            }

            assertNoRecordsToConsume();
        }
        finally {
            TestHelper.dropTable(connection, "phone");
            TestHelper.dropTable(connection, "email");
            TestHelper.dropTable(connection, "addresses");
        }
    }

}

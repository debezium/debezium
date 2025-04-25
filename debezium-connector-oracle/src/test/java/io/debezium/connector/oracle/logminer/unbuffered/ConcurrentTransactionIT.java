/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.unbuffered;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
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
import io.debezium.doc.FixFor;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * A test that verifies the unbuffered LogMiner implementation safely handles interwoven transactions.
 *
 * @author Chris Cranford
 */
@SkipWhenAdapterNameIsNot(SkipWhenAdapterNameIsNot.AdapterName.LOGMINER_UNBUFFERED)
public class ConcurrentTransactionIT extends AbstractAsyncEngineConnectorTest {

    @Rule
    public final TestRule skipAdapterRule = new SkipTestDependingOnAdapterNameRule();

    private OracleConnection connection;

    @Before
    public void beforeEach() throws Exception {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        TestHelper.dropAllTables();

        connection = TestHelper.testConnection();

        connection.execute("CREATE TABLE dbz8924_1 (id numeric(9,0) primary key, data varchar2(50))");
        TestHelper.streamTable(connection, "dbz8924_1");

        connection.execute("CREATE TABLE dbz8924_2 (id numeric(9,0) primary key, data varchar2(50))");
        TestHelper.streamTable(connection, "dbz8924_2");
    }

    @After
    public void afterEach() throws Exception {
        stopConnector();

        if (connection != null) {
            connection.close();
        }

        // TestHelper.dropAllTables();
    }

    @Test
    @FixFor("DBZ-8924")
    public void testConcurrentTransactionsHandledSuccessfully() throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ8924_1,DEBEZIUM\\.DBZ8924_2")
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForStreamingRunning(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final List<Future<Boolean>> tasks = new ArrayList<>();
            final int BATCHES = 5;
            final int BATCH_SIZE = 10000;
            final int EXPECTED = BATCHES * BATCH_SIZE;

            final Callable<Boolean> dataPump1 = () -> {
                try (OracleConnection conn = TestHelper.testConnection()) {
                    conn.setAutoCommit(false);
                    for (int j = 0; j < BATCHES; ++j) {
                        for (int i = 0; i < BATCH_SIZE; i++) {
                            final int value = (j * BATCH_SIZE) + (i + 1);
                            conn.executeWithoutCommitting("INSERT INTO dbz8924_1 values (" + value + ", '" + value + "')");
                        }
                        conn.commit();
                        Thread.sleep(250);
                    }
                }
                return true;
            };

            final Callable<Boolean> dataPump2 = () -> {
                try (OracleConnection conn = TestHelper.testConnection()) {
                    conn.setAutoCommit(false);
                    for (int j = 0; j < BATCHES; ++j) {
                        for (int i = 0; i < BATCH_SIZE; i++) {
                            final int value = (j * BATCH_SIZE) + (i + 1);
                            conn.executeWithoutCommitting("INSERT INTO dbz8924_2 values (" + value + ", '" + value + "')");
                        }
                        conn.commit();
                        Thread.sleep(500);
                    }
                }
                return true;
            };

            tasks.add(executor.submit(dataPump1));
            tasks.add(executor.submit(dataPump2));

            final List<Integer> tableIds1 = new ArrayList<>();
            final List<Integer> tableIds2 = new ArrayList<>();

            int nullValues = 0;
            while (tableIds1.size() != EXPECTED || tableIds2.size() != EXPECTED) {
                final SourceRecord record = consumeRecord();
                if (record != null) {
                    nullValues = 0;
                    int id = ((Struct) record.key()).getInt32("ID");
                    if (record.topic().equals("server1.DEBEZIUM.DBZ8924_1")) {
                        tableIds1.add(id);
                    }
                    else if (record.topic().equals("server1.DEBEZIUM.DBZ8924_2")) {
                        tableIds2.add(id);
                    }
                }
                else {
                    nullValues++;
                    assertThat(nullValues).isLessThan(10);
                }
            }

            assertThat(tableIds1).hasSize(EXPECTED);
            assertThat(tableIds2).hasSize(EXPECTED);

            for (int i = 1; i <= EXPECTED; i++) {
                assertThat(tableIds1.get(i - 1)).isEqualTo(i);
                assertThat(tableIds2.get(i - 1)).isEqualTo(i);
            }

            for (Future<Boolean> task : tasks) {
                assertThat(task.get()).isTrue();
            }

            assertNoRecordsToConsume();
        }
        finally {
            executor.shutdown();
        }
    }
}

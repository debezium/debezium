/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.actions.Log;
import io.debezium.relational.TableId;
import io.debezium.util.Testing;

public class SignalsIT extends AbstractAsyncEngineConnectorTest {

    private SqlServerConnection connection;

    @Before
    public void before() throws SQLException {
        TestHelper.createTestDatabases(TestHelper.TEST_DATABASE_1, TestHelper.TEST_DATABASE_2);
        connection = TestHelper.multiPartitionTestConnection();

        TableId db1TableA = new TableId(TestHelper.TEST_DATABASE_1, "dbo", "tableA");
        TableId db1TableB = new TableId(TestHelper.TEST_DATABASE_1, "dbo", "tableB");

        connection.execute(
                "CREATE TABLE %s (id int primary key, colA varchar(32))"
                        .formatted(connection.quotedTableIdString(db1TableA)),
                "CREATE TABLE %s (id int primary key, colB varchar(32))"
                        .formatted(connection.quotedTableIdString(db1TableB)),
                "INSERT INTO %s VALUES(1, 'a1')"
                        .formatted(connection.quotedTableIdString(db1TableA)),
                "INSERT INTO %s VALUES(2, 'b')"
                        .formatted(connection.quotedTableIdString(db1TableB)));
        TestHelper.enableTableCdc(connection, db1TableA);
        TestHelper.enableTableCdc(connection, db1TableB);

        TableId db2TableA = new TableId(TestHelper.TEST_DATABASE_2, "dbo", "tableA");
        TableId db2TableC = new TableId(TestHelper.TEST_DATABASE_2, "dbo", "tableC");

        connection.execute(
                "CREATE TABLE %s (id int primary key, colA varchar(32))"
                        .formatted(connection.quotedTableIdString(db2TableA)),
                "CREATE TABLE %s (id int primary key, colC varchar(32))"
                        .formatted(connection.quotedTableIdString(db2TableC)),
                "INSERT INTO %s VALUES(3, 'a2')"
                        .formatted(connection.quotedTableIdString(db2TableA)),
                "INSERT INTO %s VALUES(4, 'c')"
                        .formatted(connection.quotedTableIdString(db2TableC)));
        TestHelper.enableTableCdc(connection, db2TableA);
        TestHelper.enableTableCdc(connection, db2TableC);

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void jmxSignals() throws Exception {
        // Testing.Print.enable();

        final LogInterceptor logInterceptor = new LogInterceptor(Log.class);

        final Configuration config = TestHelper.defaultConfig(
                TestHelper.TEST_DATABASE_1,
                TestHelper.TEST_DATABASE_2)
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.NO_DATA)
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, "500")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "jmx")
                .with("tasks.max", 2)
                .build();

        start(SqlServerConnector.class, config);

        assertConnectorIsRunning();

        sendLogSignalWithJmx("1", "log", "{\"message\": \"Signal message at offset ''{}''\"}", "0");
        sendLogSignalWithJmx("1", "log", "{\"message\": \"Signal message at offset ''{}''\"}", "1");

        Awaitility.await("Waiting for metrics to appear").atMost(waitTimeForRecords(), TimeUnit.SECONDS)
                .until(() -> logInterceptor.countOccurrences("Signal message at offset") == 2);

        assertThat(logInterceptor.countOccurrences("Signal message at offset")).isEqualTo(2);

    }

    private void sendLogSignalWithJmx(String id, String type, String data, String taskId)
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, MBeanException {

        ObjectName objectName = new ObjectName(String.format("debezium.sql_server:type=management,context=signals,server=server1,task=%s", taskId));
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        Awaitility.await("Waiting for metrics to appear").atMost(waitTimeForRecords(), TimeUnit.SECONDS).until(() -> {
            try {
                server.getObjectInstance(objectName);
                return true;
            }
            catch (InstanceNotFoundException e) {
                return false;
            }
        });

        server.invoke(objectName, "signal", new Object[]{ id, type, data }, new String[]{ String.class.getName(), String.class.getName(), String.class.getName() });
    }
}

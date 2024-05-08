/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.util.Testing;

public class NotificationsIT extends AbstractNotificationsIT<SqlServerConnector> {

    @Before
    public void before() throws SQLException {

        TestHelper.createTestDatabase();
        SqlServerConnection sqlServerConnection = TestHelper.testConnection();
        sqlServerConnection.execute(
                "CREATE TABLE tablea (id int primary key, cola varchar(30))",
                "CREATE TABLE tableb (id int primary key, colb varchar(30))",
                "INSERT INTO tablea VALUES(1, 'a')");
        TestHelper.enableTableCdc(sqlServerConnection, "tablea");

        initializeConnectorTestFramework();

        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropTestDatabase();
    }

    protected List<String> collections() {
        return List.of("dbo.tablea", "dbo.tableb").stream().map(sch_tbl -> String.format("%s.%s", database(), sch_tbl)).collect(Collectors.toList());
    }

    @Override
    protected Class<SqlServerConnector> connectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.INITIAL);
    }

    @Override
    protected String connector() {
        return "sql_server";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER_NAME;
    }

    @Override
    protected String task() {
        return "0";
    }

    @Override
    protected String database() {
        return TestHelper.TEST_DATABASE_1;
    }

    @Override
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }

    @Test
    public void completeReadingFromACaptureInstanceNotificationEmitted() throws SQLException {
        startConnector(config -> config
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.NO_DATA)
                .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification")
                .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink"));

        assertConnectorIsRunning();

        TestHelper.waitForStreamingStarted();

        SqlServerConnection connection = TestHelper.testConnection();
        connection.execute("INSERT INTO tablea VALUES(2, 'b')");
        connection.execute("ALTER TABLE tablea ADD colb int NULL");
        TestHelper.enableTableCdc(connection, "tablea", "tablea_c2");
        connection.execute("INSERT INTO tablea VALUES(3, 'c', 3)");
        connection.close();

        List<SourceRecord> notifications = new ArrayList<>();
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            consumeAvailableRecords(r -> {
                if (r.topic().equals("io.debezium.notification")) {
                    notifications.add(r);
                }
            });
            return notifications.size() == 3;
        });

        Assertions.assertThat(notifications).hasSize(3);
        SourceRecord sourceRecord = notifications.get(2);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        Struct value = (Struct) sourceRecord.value();
        Assertions.assertThat(value.getString("aggregate_type")).isEqualTo("Capture Instance");
        Assertions.assertThat(value.getString("type")).isEqualTo("COMPLETED");
        Assertions.assertThat(value.getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
        Map<String, String> additionalData = value.getMap("additional_data");
        Assertions.assertThat(additionalData.get("server")).isEqualTo("server1");
        Assertions.assertThat(additionalData.get("database")).isEqualTo(TestHelper.TEST_DATABASE_1);
        Assertions.assertThat(additionalData.get("capture_instance")).isEqualTo("dbo_tablea");
        Lsn startLsn = Lsn.valueOf(additionalData.get("start_lsn"));
        Lsn stopLsn = Lsn.valueOf(additionalData.get("stop_lsn"));
        Lsn commitLsn = Lsn.valueOf(additionalData.get("commit_lsn"));
        Assertions.assertThat(startLsn).isLessThan(stopLsn);
        Assertions.assertThat(stopLsn).isLessThan(commitLsn);

        connection = TestHelper.testConnection();
        connection.execute("EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N'tablea', @capture_instance = 'dbo_tablea'");
        connection.execute("ALTER TABLE tablea ADD colc int NULL");
        TestHelper.enableTableCdc(connection, "tablea", "tablea_c3");
        connection.execute("INSERT INTO tablea VALUES(4, 'c', 4, 4)");
        connection.close();

        List<SourceRecord> notifications2 = new ArrayList<>();
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
            consumeAvailableRecords(r -> {
                if (r.topic().equals("io.debezium.notification")) {
                    notifications2.add(r);
                }
            });
            return notifications2.size() == 1;
        });

        Assertions.assertThat(notifications2).hasSize(1);
        sourceRecord = notifications2.get(0);
        Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
        value = (Struct) sourceRecord.value();
        Assertions.assertThat(value.getString("aggregate_type")).isEqualTo("Capture Instance");
        Assertions.assertThat(value.getString("type")).isEqualTo("COMPLETED");
        Assertions.assertThat(value.getInt64("timestamp")).isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
        additionalData = value.getMap("additional_data");
        Assertions.assertThat(additionalData.get("server")).isEqualTo("server1");
        Assertions.assertThat(additionalData.get("database")).isEqualTo(TestHelper.TEST_DATABASE_1);
        Assertions.assertThat(additionalData.get("capture_instance")).isEqualTo("tablea_c2");
        startLsn = Lsn.valueOf(additionalData.get("start_lsn"));
        stopLsn = Lsn.valueOf(additionalData.get("stop_lsn"));
        commitLsn = Lsn.valueOf(additionalData.get("commit_lsn"));
        Assertions.assertThat(startLsn).isLessThan(stopLsn);
        Assertions.assertThat(stopLsn).isLessThan(commitLsn);
    }
}

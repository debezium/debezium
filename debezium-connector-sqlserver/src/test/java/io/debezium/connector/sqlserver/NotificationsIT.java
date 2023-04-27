/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.pipeline.notification.AbstractNotificationsIT;
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

    @Override
    protected Class<SqlServerConnector> connectorClass() {
        return SqlServerConnector.class;
    }

    @Override
    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SqlServerConnectorConfig.SnapshotMode.SCHEMA_ONLY);
    }

    @Override
    protected String snapshotStatusResult() {
        return "COMPLETED";
    }
}

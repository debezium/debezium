/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.BeforeClass;

import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig.SnapshotMode;
import io.debezium.connector.sqlserver.util.TestHelper;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes.
 * The types are discovered during snapshotting phase.
 *
 * @author Jiri Pechanec
 */
public class DatatypesFromSnapshotIT extends AbstractSqlServerDatatypesTest {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        createTables();
    }

    @Before
    public void before() throws Exception {
        initializeConnectorTestFramework();
        Testing.Debug.enable();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);

        Configuration config = TestHelper.defaultConfig()
                .with(SqlServerConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();
        start(SqlServerConnector.class, config);
        assertConnectorIsRunning();
        Thread.sleep(1000);
    }
}

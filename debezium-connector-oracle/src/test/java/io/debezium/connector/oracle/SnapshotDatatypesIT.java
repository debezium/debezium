/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes.
 *
 * @author Jiri Pechanec
 */
public class SnapshotDatatypesIT extends AbstractOracleDatatypesTest {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        createTables();
    }

    @Before
    public void before() throws Exception {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Debug.enable();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);

        String whitelistedTables = getAllTables().stream()
                .map(t -> "ORCLPDB1." + t)
                .map(t -> t.replaceAll("\\.", "\\\\."))
                .collect(Collectors.joining(","));

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_WHITELIST, whitelistedTables)
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL_SCHEMA_ONLY)
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(2000);
    }
}

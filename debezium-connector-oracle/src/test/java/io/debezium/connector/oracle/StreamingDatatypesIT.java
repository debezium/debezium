/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes as captured during streaming.
 *
 * @author Jiri Pechanec
 */
public class StreamingDatatypesIT extends AbstractOracleDatatypesTest {

    @Before
    public void before() throws Exception {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        dropTables();
        initializeConnectorTestFramework();
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

        // wait until snapshotting has completed
        // TODO add hook to embedded engine to reliably do this
        Thread.sleep(2000);
        createTables();
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return true;
    }
}

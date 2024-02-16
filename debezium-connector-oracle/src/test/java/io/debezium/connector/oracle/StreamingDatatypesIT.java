/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.oracle.OracleConnectorConfig.SnapshotMode;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes as captured during streaming.
 *
 * @author Jiri Pechanec
 */
public class StreamingDatatypesIT extends AbstractOracleDatatypesTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Before
    public void before() throws Exception {
        init(TemporalPrecisionMode.ADAPTIVE);
    }

    @Override
    protected void init(TemporalPrecisionMode temporalPrecisionMode) throws Exception {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        dropTables();
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        Configuration config = connectorConfig()
                .with(OracleConnectorConfig.TIME_PRECISION_MODE, temporalPrecisionMode)
                .with(OracleConnectorConfig.LOB_ENABLED, true)
                .build();

        createTables();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    protected Builder connectorConfig() {
        String tableIncludeList = getAllTables().stream()
                .map(t -> t.replaceAll("\\.", "\\\\."))
                .collect(Collectors.joining(","));

        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                .with(OracleConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA);
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return true;
    }
}

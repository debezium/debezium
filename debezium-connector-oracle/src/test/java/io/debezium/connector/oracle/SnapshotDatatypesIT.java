/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes as captured during initial snapshotting.
 *
 * @author Jiri Pechanec
 */
public class SnapshotDatatypesIT extends AbstractOracleDatatypesTest {

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void beforeClass() throws SQLException {
        AbstractOracleDatatypesTest.beforeClass();
        createTables();

        insertStringTypes();
        insertFpTypes();
        insertIntTypes();
        insertTimeTypes();
    }

    @Before
    public void before() throws Exception {
        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Debug.enable();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);

        Configuration config = connectorConfig()
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    protected Builder connectorConfig() {
        return TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, getTableIncludeList());
    }

    private String getTableIncludeList() {
        switch (name.getMethodName()) {
            case "stringTypes":
                return "debezium.type_string";
            case "fpTypes":
            case "fpTypesAsString":
            case "fpTypesAsDouble":
                return "debezium.type_fp";
            case "intTypes":
                return "debezium.type_int";
            case "timeTypes":
                return "debezium.type_time";
            default:
                throw new IllegalArgumentException("Unexpected test method: " + name.getMethodName());
        }
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return false;
    }
}

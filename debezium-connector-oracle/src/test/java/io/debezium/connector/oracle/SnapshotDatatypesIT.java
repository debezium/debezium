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
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes as captured during initial snapshotting.
 *
 * @author Jiri Pechanec
 */
public class SnapshotDatatypesIT extends AbstractOracleDatatypesTest {

    @Rule public TestName name = new TestName();

    @BeforeClass
    public static void beforeClass() throws SQLException {
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

        Configuration config = TestHelper.defaultConfig()
                .with(OracleConnectorConfig.TABLE_WHITELIST, getTableWhitelist())
                .build();

        start(OracleConnector.class, config);
        assertConnectorIsRunning();

        Thread.sleep(2000);
    }

    private String getTableWhitelist() {
        switch(name.getMethodName()) {
            case "stringTypes":
                return "ORCLPDB1.debezium.type_string";
            case "fpTypes":
                return "ORCLPDB1.debezium.type_fp";
            case "intTypes":
                return "ORCLPDB1.debezium.type_int";
            case "timeTypes":
                return "ORCLPDB1.debezium.type_time";
            default:
                throw new IllegalArgumentException("Unexpected test method: " + name.getMethodName());
        }
    }

    @Override
    protected boolean insertRecordsDuringTest() {
        return false;
    }
}

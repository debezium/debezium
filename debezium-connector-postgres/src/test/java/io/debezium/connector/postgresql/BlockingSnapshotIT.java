/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractBlockingSnapshotTest;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

public class BlockingSnapshotIT extends AbstractBlockingSnapshotTest {

    private static final String TOPIC_NAME = "test_server.s1.a";

    private static final String SETUP_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "CREATE SCHEMA s1;CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s1.debezium_signal (id varchar(64), type varchar(32), data varchar(2048))";

    @Before
    public void before() throws SQLException {

        TestHelper.dropAllSchemas();
        TestHelper.dropDefaultReplicationSlot();
        initializeConnectorTestFramework();

        TestHelper.createDefaultReplicationSlot();
        TestHelper.execute(SETUP_TABLES_STMT);
        TestHelper.createPublicationForAllTables();
    }

    @After
    public void after() {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

    }

    protected Configuration.Builder config() {
        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.SIGNAL_DATA_COLLECTION, "s1.debezium_signal")
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(CommonConnectorConfig.SIGNAL_ENABLED_CHANNELS, "source")
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(RelationalDatabaseConnectorConfig.MSG_KEY_COLUMNS, "s1.a42:pk1,pk2,pk3,pk4");
    }

    @Override
    protected Configuration.Builder mutableConfig(boolean signalTableOnly, boolean storeOnlyCapturedDdl) {

        return TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL.getValue())
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, Boolean.FALSE)
                .with(PostgresConnectorConfig.SIGNAL_DATA_COLLECTION, "s1.debezium_signal")
                .with(CommonConnectorConfig.SIGNAL_POLL_INTERVAL_MS, 5)
                .with(PostgresConnectorConfig.INCREMENTAL_SNAPSHOT_CHUNK_SIZE, 10)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "s1")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, "s1.a");
    }

    @Override
    protected Class<PostgresConnector> connectorClass() {
        return PostgresConnector.class;
    }

    @Override
    protected JdbcConnection databaseConnection() {
        return TestHelper.create();
    }

    @Override
    protected String topicName() {
        return TOPIC_NAME;
    }

    @Override
    public List<String> topicNames() {
        return List.of(TOPIC_NAME, "test_server.s1.b");
    }

    @Override
    protected String tableName() {
        return "s1.a";
    }

    @Override
    protected List<String> tableNames() {
        return List.of("s1.a", "s1.b");
    }

    @Override
    protected String signalTableName() {
        return "s1.debezium_signal";
    }

    @Override
    protected String connector() {
        return "postgres";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER;
    }

}

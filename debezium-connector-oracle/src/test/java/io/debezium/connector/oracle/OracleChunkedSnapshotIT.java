/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;
import io.debezium.util.Testing;

/**
 * Oracle-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
public class OracleChunkedSnapshotIT extends AbstractChunkedSnapshotTest<OracleConnector> {

    private OracleConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = TestHelper.testConnection();
        TestHelper.dropAllTables();

        setConsumeTimeout(TestHelper.defaultMessageConsumerPollTimeout(), TimeUnit.SECONDS);
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.SCHEMA_HISTORY_PATH);

        super.beforeEach();
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.afterEach();
    }

    @Override
    protected void populateSingleKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateSingleKeyTable(tableName, rowCount);
        TestHelper.streamTable(connection, tableName);
    }

    @Override
    protected void populateCompositeKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateCompositeKeyTable(tableName, rowCount);
        TestHelper.streamTable(connection, tableName);
    }

    @Override
    protected Class<OracleConnector> getConnectorClass() {
        return OracleConnector.class;
    }

    @Override
    protected JdbcConnection getConnection() {
        return connection;
    }

    @Override
    protected Configuration.Builder getConfig() {
        return TestHelper.defaultConfig();
    }

    @Override
    protected void waitForSnapshotToBeCompleted() throws InterruptedException {
        waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    }

    @Override
    protected String getSingleKeyCollectionName() {
        return "DEBEZIUM\\.DBZ1220";
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getSingleKeyCollectionName();
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return String.join(",", List.of("DEBEZIUM\\.DBZ1220A", "DEBEZIUM\\.DBZ1220B", "DEBEZIUM\\.DBZ1220C", "DEBEZIUM\\.DBZ1220D"));
    }

    @Override
    protected void createSingleKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0) primary key, data varchar2(50))".formatted(tableName));
    }

    @Override
    protected void createCompositeKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), org_name varchar2(50), data varchar2(50), primary key(id, org_name))".formatted(tableName));
    }

    @Override
    protected void createKeylessTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), data varchar2(50))".formatted(tableName));
    }

    @Override
    protected String getSingleKeyTableKeyColumnName() {
        return "ID";
    }

    @Override
    protected List<String> getCompositeKeyTableKeyColumnNames() {
        return List.of("ID", "ORG_NAME");
    }

    @Override
    protected String getTableTopicName(String tableName) {
        return "server1.DEBEZIUM.%s".formatted(tableName.toUpperCase());
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return "%s.DEBEZIUM.%s".formatted(TestHelper.getDatabaseName(), tableName.toUpperCase());
    }

    // @Test
    // @FixFor("dbz#1220")
    // @Disabled
    // public void shouldSnapshotTableAcrossMultipleThreads() throws Exception {
    // TestHelper.dropTable(connection, "dbz1220");
    // try {
    // final int ROW_COUNT = 10_000_000;
    //
    // // Create table and populate
    // connection.execute("CREATE TABLE dbz1220 (id numeric(9,0), data varchar2(50), PRIMARY KEY(id))");
    // try (PreparedStatement st = connection.connection().prepareStatement("INSERT INTO dbz1220 VALUES (?,?)")) {
    // for (int i = 0; i < ROW_COUNT; i++) {
    // st.setInt(1, i);
    // st.setString(2, String.valueOf(i));
    // st.addBatch();
    // }
    // st.executeBatch();
    // }
    // connection.commit();
    // TestHelper.streamTable(connection, "dbz1220");
    //
    // Configuration config = TestHelper.defaultConfig()
    // .with(OracleConnectorConfig.SNAPSHOT_MAX_THREADS, 20)// 5)
    // .with(OracleConnectorConfig.SNAPSHOT_MAX_THREADS_MULTIPLIER, 5) // 2)
    // .with(CommonConnectorConfig.MAX_BATCH_SIZE, ROW_COUNT)
    // .with(OracleConnectorConfig.MAX_QUEUE_SIZE, ROW_COUNT * 2)
    // .with(OracleConnectorConfig.TABLE_INCLUDE_LIST, "DEBEZIUM\\.DBZ1220")
    // .build();
    //
    // final LogInterceptor logInterceptor = new LogInterceptor(RelationalSnapshotChangeEventSource.class);
    //
    // start(OracleConnector.class, config);
    // assertConnectorIsRunning();
    //
    // waitForSnapshotToBeCompleted(TestHelper.CONNECTOR_NAME, TestHelper.SERVER_NAME);
    //
    // final List<SourceRecord> data = new ArrayList<>();
    // while (data.size() < ROW_COUNT) {
    // data.addAll(consumeRecordsByTopic(1).recordsForTopic("server1.DEBEZIUM.DBZ1220"));
    // }
    //
    // final Set<Integer> ids = data.stream().map(r -> {
    // Struct after = ((Struct) r.value()).getStruct(Envelope.FieldName.AFTER);
    // return after.getInt32("ID");
    // }).collect(Collectors.toSet());
    //
    // assertThat(ids).hasSize(ROW_COUNT);
    // }
    // finally {
    // TestHelper.dropTable(connection, "dbz1220");
    // }
    // }

}

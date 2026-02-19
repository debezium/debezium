/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.AbstractChunkedSnapshotTest;

/**
 * PostgreSQL-specific chunked table snapshot integration tests.
 *
 * @author Chris Cranford
 */
public class PostgresChunkedSnapshotIT extends AbstractChunkedSnapshotTest<PostgresConnector> {

    private PostgresConnection connection;

    @BeforeEach
    public void beforeEach() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();

        TestHelper.createDefaultReplicationSlot();
        TestHelper.createPublicationForAllTables();
        initializeConnectorTestFramework();

        connection = TestHelper.create();

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
    }

    @Override
    protected void populateCompositeKeyTable(String tableName, int rowCount) throws SQLException {
        super.populateCompositeKeyTable(tableName, rowCount);
    }

    @Override
    protected Class<PostgresConnector> getConnectorClass() {
        return PostgresConnector.class;
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
        waitForSnapshotToBeCompleted("postgres", TestHelper.TEST_SERVER);
    }

    @Override
    protected String connector() {
        return "postgres";
    }

    @Override
    protected String server() {
        return TestHelper.TEST_SERVER;
    }

    @Override
    protected String getSingleKeyCollectionName() {
        return "public.dbz1220";
    }

    @Override
    protected String getCompositeKeyCollectionName() {
        return getSingleKeyCollectionName();
    }

    @Override
    protected String getMultipleSingleKeyCollectionNames() {
        return String.join(",", List.of("public.dbz1220a", "public.dbz1220b", "public.dbz1220c", "public.dbz1220d"));
    }

    @Override
    protected void createSingleKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0) primary key, data varchar(50))".formatted(tableName));
    }

    @Override
    protected void createCompositeKeyTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), org_name varchar(50), data varchar(50), primary key(id, org_name))".formatted(tableName));
    }

    @Override
    protected void createKeylessTable(String tableName) throws SQLException {
        connection.execute("CREATE TABLE %s (id numeric(9,0), data varchar(50))".formatted(tableName));
    }

    @Override
    protected String getSingleKeyTableKeyColumnName() {
        return "id";
    }

    @Override
    protected List<String> getCompositeKeyTableKeyColumnNames() {
        return List.of("id", "org_name");
    }

    @Override
    protected String getTableTopicName(String tableName) {
        return "test_server.%s.%s".formatted("public", tableName);
    }

    @Override
    protected String getFullyQualifiedTableName(String tableName) {
        return "public.%s".formatted(tableName);
    }

}

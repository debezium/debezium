/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

import org.junit.Ignore;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogVectorToJsonConverterIT;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * @author Chris Cranford
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 9, minor = 0, reason = "VECTOR datatype not added until MySQL 9.0")
public class MysqlVectorToJsonConverterIT extends BinlogVectorToJsonConverterIT<MySqlConnector> implements MySqlCommon {
    @Override
    public Class<MySqlConnector> getConnectorClass() {
        return MySqlConnector.class;
    }

    @Override
    protected Configuration.Builder getConfigurationBuilder() {
        return DATABASE.defaultConfig()
                .with(BinlogConnectorConfig.USER, "mysqluser")
                .with(BinlogConnectorConfig.PASSWORD, "mysqlpw")
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false);
    }

    @Override
    protected String topicName() {
        return DATABASE.topicForTable("dbz8571");
    }

    @Override
    protected void createFloatVectorTable() throws Exception {
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            connection.execute("CREATE TABLE dbz8571 (id int not null, data vector, primary key(id))");
        }
    }

    @Override
    protected void createFloatVectorSnapshotData() throws Exception {
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            connection.execute("INSERT INTO dbz8571 (id,data) values (1, string_to_vector('[101,102,103]'))");
        }
    }

    @Override
    protected void createFloatVectorStreamData() throws Exception {
        try (JdbcConnection connection = databaseConnection()) {
            connection.setAutoCommit(true);
            connection.execute("INSERT INTO dbz8571 (id,data) values (2, string_to_vector('[1,2,3]'))");
            connection.execute("UPDATE dbz8571 set data = string_to_vector('[5,7,9]') WHERE id = 2");
            connection.execute("DELETE FROM dbz8571 WHERE id = 2");
        }
    }

    @Override
    protected void createDoubleVectorTable() throws Exception {
        throw new UnsupportedOperationException("MySQL does not support double vector types");
    }

    @Override
    protected void createDoubleVectorSnapshotData() throws Exception {
        throw new UnsupportedOperationException("MySQL does not support double vector types");
    }

    @Override
    protected void createDoubleVectorStreamData() throws Exception {
        throw new UnsupportedOperationException("MySQL does not support double vector types");
    }

    @Override
    protected void waitForStreamingStarted() throws InterruptedException {
        waitForStreamingRunning(getConnectorName(), DATABASE.getServerName());
    }

    @Override
    @Ignore("MySQL does not support the use of DoubleVector logical types")
    public void shouldConvertDoubleVectorToJson() throws Exception {
    }

}

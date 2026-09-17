/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.kafka.connect.source.SourceConnector;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.util.BinlogTestConnection;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipWhenDatabaseVersion;

/**
 * @author Chris Cranford
 */
@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, patch = 5, reason = "MySQL 5.5 does not support CURRENT_TIMESTAMP on DATETIME and only a single column can specify default CURRENT_TIMESTAMP, lifted in MySQL 5.6.5")
public abstract class BinlogConnectionIT<C extends SourceConnector> extends AbstractBinlogConnectorIT<C> {

    @Disabled
    @Test
    void shouldConnectToDefaultDatabase() throws SQLException {
        try (BinlogTestConnection conn = getTestDatabaseConnection("mysql")) {
            conn.connect();
        }
    }

    @Test
    void shouldDoStuffWithDatabase() throws SQLException {
        final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("readbinlog", "readbinlog_test");
        DATABASE.createAndInitialize();
        try (BinlogTestConnection conn = getTestDatabaseConnection(DATABASE.getDatabaseName());) {
            conn.connect();
            // Set up the table as one transaction and wait to see the events ...
            conn.execute("DROP TABLE IF EXISTS person",
                    "CREATE TABLE person ("
                            + "  name VARCHAR(255) primary key,"
                            + "  birthdate DATE NULL,"
                            + "  age INTEGER NULL DEFAULT 10,"
                            + "  salary DECIMAL(5,2),"
                            + "  bitStr BIT(18)"
                            + ")");
            conn.execute("SELECT * FROM person");
            try (ResultSet rs = conn.connection().getMetaData().getColumns("readbinlog_test", null, null, null)) {
                // if ( Testing.Print.isEnabled() ) conn.print(rs);
            }
        }
    }

    @Disabled
    @Test
    void shouldConnectToEmptyDatabase() throws SQLException {
        try (BinlogTestConnection conn = getTestDatabaseConnection("emptydb")) {
            conn.connect();
        }
    }

    @Test
    @FixFor("debezium/dbz#2452")
    void shouldClassifyUndefinedColumnError() throws SQLException {
        final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("readbinlog", "readbinlog_test");
        DATABASE.createAndInitialize();
        try (BinlogConnectorConnection connection = connectorConnection(DATABASE.defaultConfig().build())) {
            connection.connect();
            try {
                connection.execute("SELECT no_such_column FROM information_schema.tables");
                fail("the query on a non-existing column should have failed");
            }
            catch (SQLException e) {
                assertThat(connection.isUndefinedColumnError(e)).isTrue();
            }
            try {
                connection.execute("SELECT 1 FROM no_such_table_2452");
                fail("the query on a non-existing table should have failed");
            }
            catch (SQLException e) {
                assertThat(connection.isUndefinedColumnError(e)).isFalse();
            }
        }
    }

    /**
     * The connector's own connection for the given connector configuration: the classifier under test lives on
     * it, not on the plain {@link BinlogTestConnection} the other tests use.
     */
    protected abstract BinlogConnectorConnection connectorConnection(Configuration config);
}

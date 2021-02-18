/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import io.debezium.junit.SkipTestRule;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.util.Testing;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, patch = 5, reason = "MySQL 5.5 does not support CURRENT_TIMESTAMP on DATETIME and only a single column can specify default CURRENT_TIMESTAMP, lifted in MySQL 5.6.5")
public class ConnectionIT implements Testing {

    @Rule
    public SkipTestRule skipTest = new SkipTestRule();

    @Ignore
    @Test
    public void shouldConnectToDefaultDatabase() throws SQLException {
        try (MySqlTestConnection conn = MySqlTestConnection.forTestDatabase("mysql");) {
            conn.connect();
        }
    }

    @Test
    public void shouldDoStuffWithDatabase() throws SQLException {
        final UniqueDatabase DATABASE = new UniqueDatabase("readbinlog", "readbinlog_test");
        DATABASE.createAndInitialize();
        try (MySqlTestConnection conn = MySqlTestConnection.forTestDatabase(DATABASE.getDatabaseName());) {
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

    @Ignore
    @Test
    public void shouldConnectToEmptyDatabase() throws SQLException {
        try (MySqlTestConnection conn = MySqlTestConnection.forTestDatabase("emptydb");) {
            conn.connect();
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.debezium.connector.cube.DatabaseCube;
import io.debezium.connector.mysql.cube.DefaultDatabase;
import io.debezium.util.Testing;

@RunWith(Arquillian.class)
public class ConnectionIT implements Testing {

    @DefaultDatabase
    private DatabaseCube cube;

    @Ignore
    @Test
    public void shouldConnectToDefaulDatabase() throws SQLException {
        try (MySQLConnection conn = MySQLConnection.forTestDatabase("mysql", cube.getHost(), cube.getPort())) {
            conn.connect();
        }
    }

    @Test
    public void shouldDoStuffWithDatabase() throws SQLException {
        try (MySQLConnection conn = MySQLConnection.forTestDatabase("readbinlog_test", cube.getHost(), cube.getPort())) {
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
                //if ( Testing.Print.isEnabled() ) conn.print(rs);
            }
        }
    }

    @Ignore
    @Test
    public void shouldConnectToEmptyDatabase() throws SQLException {
        try (MySQLConnection conn = MySQLConnection.forTestDatabase("emptydb", cube.getHost(), cube.getPort())) {
            conn.connect();
        }
    }
}

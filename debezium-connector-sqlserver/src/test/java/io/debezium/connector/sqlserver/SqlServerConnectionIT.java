/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.util.Testing;

/**
 * Integration test for {@link SqlServerConnection}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class SqlServerConnectionIT {

    @BeforeClass
    public static void beforeClass() throws SQLException {
        // NOTE: you cannot enable CDC for the "master" db (the default one) so
        // all tests must use a separate database...
        try (SqlServerConnection connection = new SqlServerConnection(TestHelper.defaultJdbcConfig())) {
            connection.connect();
            String sql = "IF EXISTS(select 1 from sys.databases where name='testDB') DROP DATABASE testDB\n"
                    + "CREATE DATABASE testDB\n";
            connection.execute(sql);
        }
    }

    @Test
    public void shouldEnableCDCForDatabase() throws Exception {
        try (SqlServerConnection connection = new SqlServerConnection(TestHelper.defaultJdbcConfig())) {
            connection.connect();
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            connection.enableDbCdc("testDB");
        }
    }

    @Test
    public void shouldEnableCDCWithWrapperFunctionsForTable() throws Exception {
        try (SqlServerConnection connection = new SqlServerConnection(TestHelper.defaultJdbcConfig())) {
            connection.connect();
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            connection.enableDbCdc("testDB");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects where name = 'testTable' and type = 'u')\n"
                    + "DROP TABLE testTable\n"
                    + "CREATE TABLE testTable (ID int not null identity(1, 1) primary key, NUMBER int, TEXT text)";
            connection.execute(sql);

            // then enable CDC and wrapper functions
            connection.enableTableCdc("testTable");
            // insert some data

            connection.execute("INSERT INTO testTable (NUMBER, TEXT) values (1, 'aaa')\n"
                    + "INSERT INTO testTable (NUMBER, TEXT) values (2, 'bbb')");

            // and issue a test call to a CDC wrapper function
            Thread.sleep(5_000); // Need to wait to make sure the min_lsn is available

            Testing.Print.enable();
            connection.query(
                    "select * from cdc.fn_cdc_get_all_changes_dbo_testTable(sys.fn_cdc_get_min_lsn('dbo_testTable'), sys.fn_cdc_get_max_lsn(), N'all')",
                    rs -> {
                        while (rs.next()) {
                            final StringBuilder sb = new StringBuilder();
                            for (int col = 1; col <= rs.getMetaData().getColumnCount(); col++) {
                                sb.append(rs.getObject(col)).append(' ');
                            }
                            Testing.print(sb.toString());
                        }
                    });
            Testing.Print.disable();
        }

    }

}

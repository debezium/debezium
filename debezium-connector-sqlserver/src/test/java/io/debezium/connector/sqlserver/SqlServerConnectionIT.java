/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.sqlserver;

import java.sql.SQLException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration test for {@link SqlServerConnection}
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class SqlServerConnectionIT {
    
    @BeforeClass
    public static void beforeClass() throws SQLException {
        //NOTE: you cannot enable CDC for the "master" db (the default one) so all tests must use a separate database...
        try (SqlServerConnection connection = new SqlServerConnection(TestHelper.defaultJdbcConfig())) {
            connection.connect();
            String sql = "IF EXISTS(select 1 from sys.databases where name='testDB') DROP DATABASE testDB\n" +
                         "CREATE DATABASE testDB\n";
            connection.execute(sql);
        }
    }
    
    @Test
    public void shouldEnableCDCForDatabase() throws Exception {
        try (SqlServerConnection connection = new SqlServerConnection(TestHelper.defaultJdbcConfig())) {
            connection.connect();
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            connection.enableDBCDC("testDB");
        }       
    }
    
    @Test
    public void shouldEnableCDCWithWrapperFunctionsForTable() throws Exception {
        try (SqlServerConnection connection = new SqlServerConnection(TestHelper.defaultJdbcConfig())) {
            connection.connect();
            connection.execute("USE testDB");
            // NOTE: you cannot enable CDC on master
            connection.enableDBCDC("testDB");

            // create table if exists
            String sql = "IF EXISTS (select 1 from sys.objects where name = 'testTable' and type = 'u')\n" +
                         "DROP TABLE testTable\n" + 
                         "CREATE TABLE testTable (ID int not null identity(1, 1) primary key, NUMBER int, TEXT text)";
            connection.execute(sql);
            
            // then enable CDC and wrapper functions
            connection.enableTableCDC("testTable");
            
            // insert some data
            
            connection.execute("INSERT INTO testTable (NUMBER, TEXT) values (1, 'aaa')\n" +
                               "INSERT INTO testTable (NUMBER, TEXT) values (2, 'bbb')");
            // and issue a test call to a CDC wrapper function
            /**
             * The following fails if CDC is not supported on a particular SQL Server instance
             * 
            connection.call("cdc.fn_all_changes_dbo_testTable(?,?,?)", call -> {
                call.setDate(1, null);
                call.setDate(2, null);
                call.setString(3, "all");
            }, null);
             */
        }
    
    }
    
}

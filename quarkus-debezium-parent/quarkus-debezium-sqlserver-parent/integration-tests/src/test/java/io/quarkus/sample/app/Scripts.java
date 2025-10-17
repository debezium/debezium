/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import java.sql.SQLException;
import java.util.Collections;

import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;

public class Scripts {
    public static final String DEFAULT = """
            CREATE DATABASE native
            USE native

            WAITFOR DELAY '00:00:30'

            EXEC sys.sp_cdc_enable_db


            CREATE TABLE users(id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(255) NOT NULL)
            CREATE TABLE products(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(255) NOT NULL)

            INSERT INTO users (name) VALUES ('alvar'), ('anisha'), ('chris'), ('indra'), ('jiri'), ('giovanni'), ('mario'), ('rené'), ('Vojtěch')
            INSERT INTO products (id, name, description) VALUES (1, 't-shirt','red hat t-shirt'), (2,'sweatshirt','blue ibm sweatshirt')

            EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'products', @role_name = NULL, @supports_net_changes = 0
            EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'users', @role_name = NULL, @supports_net_changes = 0
            """;
    public static final String ALTERNATIVE = """
            CREATE DATABASE alternative
            USE alternative

            WAITFOR DELAY '00:00:30'

            EXEC sys.sp_cdc_enable_db

            CREATE TABLE orders(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(255) NOT NULL)

            INSERT INTO orders(id, name, description) VALUES (1, 'pizza','pizza with peperoni'), (2,'kebab','kebab with mayonnaise')

            EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'orders', @role_name = NULL, @supports_net_changes = 0
            """;

    public static void applySql(SqlServerConnectorConfig defaultConfig, String script) throws SQLException {
        try (SqlServerConnection defaultConnection = new SqlServerConnection(defaultConfig,
                new SqlServerValueConverters(JdbcValueConverters.DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE, null),
                Collections.emptySet(), false)) {

            JdbcConnection connect = defaultConnection.connect();
            connect.execute(script.split("\n"));
        }
    }
}

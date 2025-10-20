/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerValueConverters;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;

public class Scripts {

    private static final Set<String> configurations = new HashSet<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(Scripts.class);

    public static void apply(Database configuration) throws SQLException {
        if (configurations.contains(configuration.name())) {
            return;
        }
        LOGGER.info("Applying script to {}", configuration.name());
        configurations.add(configuration.name());

        try (SqlServerConnection defaultConnection = new SqlServerConnection(configuration.sqlServerConnectorConfig,
                new SqlServerValueConverters(JdbcValueConverters.DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE, null),
                Collections.emptySet(), false)) {

            JdbcConnection connect = defaultConnection.connect();
            connect.execute(configuration.script.split("\n"));
        }
    }

    public enum Database {
        DEFAULT(new SqlServerConnectorConfig(
                JdbcConfiguration.copy(Configuration.fromSystemProperties(ConfigurationNames.DATABASE_CONFIG_PREFIX))
                        .with("database.hostname", "localhost")
                        .with("database.port", 1433)
                        .with("database.user", "sa")
                        .with("database.encrypt", "false")
                        .with("database.trustServerCertificate", "false")
                        .with("database.password", "Password!")
                        .build()),
                Constants.DEFAULT_SCRIPT),

        ALTERNATIVE(new SqlServerConnectorConfig(
                JdbcConfiguration.copy(Configuration.fromSystemProperties(ConfigurationNames.DATABASE_CONFIG_PREFIX))
                        .with("database.hostname", "localhost")
                        .with("database.port", 1434)
                        .with("database.user", "sa")
                        .with("database.encrypt", "false")
                        .with("database.trustServerCertificate", "false")
                        .with("database.password", "Password!")
                        .build()),
                Constants.ALTERNATIVE_SCRIPT);

        private final SqlServerConnectorConfig sqlServerConnectorConfig;
        private final String script;

        Database(SqlServerConnectorConfig config, String script) {
            this.sqlServerConnectorConfig = config;
            this.script = script;
        }

        public SqlServerConnectorConfig getSqlServerConnectorConfig() {
            return sqlServerConnectorConfig;
        }

        public String getScript() {
            return script;
        }

        private static class Constants {
            public static final String ALTERNATIVE_SCRIPT = """
                    CREATE DATABASE alternative
                    USE alternative

                    WAITFOR DELAY '00:00:30'

                    EXEC sys.sp_cdc_enable_db

                    CREATE TABLE orders(id INT NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, description VARCHAR(255) NOT NULL)

                    INSERT INTO orders(id, name, description) VALUES (1, 'pizza','pizza with peperoni'), (2,'kebab','kebab with mayonnaise')

                    EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'orders', @role_name = NULL, @supports_net_changes = 0
                    """;
            public static final String DEFAULT_SCRIPT = """
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
        }
    }
}

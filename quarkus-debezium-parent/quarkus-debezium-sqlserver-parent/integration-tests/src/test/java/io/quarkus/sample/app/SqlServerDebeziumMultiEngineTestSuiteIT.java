/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import static io.quarkus.sample.app.Scripts.applySql;

import java.sql.SQLException;

import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;

public class SqlServerDebeziumMultiEngineTestSuiteIT {

    public static void init() throws SQLException {
        SqlServerConnectorConfig defaultConfig = new SqlServerConnectorConfig(
                JdbcConfiguration.copy(Configuration.fromSystemProperties(ConfigurationNames.DATABASE_CONFIG_PREFIX))
                        .with("database.hostname", "localhost")
                        .with("database.port", 1433)
                        .with("database.user", "sa")
                        .with("database.encrypt", "false")
                        .with("database.trustServerCertificate", "false")
                        .with("database.password", "Password!")
                        .build());

        SqlServerConnectorConfig alternativeConfig = new SqlServerConnectorConfig(
                JdbcConfiguration.copy(Configuration.fromSystemProperties(ConfigurationNames.DATABASE_CONFIG_PREFIX))
                        .with("database.hostname", "localhost")
                        .with("database.port", 1434)
                        .with("database.user", "sa")
                        .with("database.encrypt", "false")
                        .with("database.trustServerCertificate", "false")
                        .with("database.password", "Password!")
                        .build());

        applySql(defaultConfig, Scripts.DEFAULT);
        applySql(alternativeConfig, Scripts.ALTERNATIVE);
    }

}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.perf;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import io.debezium.junit.SkipLongRunning;

@SkipLongRunning
public class MySqlConnectorTest extends AbstractPerformanceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorTest.class);

    @Override
    protected JdbcConnection databaseConnection() {
        final ConnectionFactory connectionFactory = JdbcConnection.patternBasedFactory(
                "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL");
        final Configuration config = config().databaseConfiguration().edit()
                .withDefault("hostname", "localhost")
                .withDefault("port", "3306")
                .withDefault("user", "root")
                .withDefault("password", "debezium")
                .build();
        return new JdbcConnection(config, connectionFactory, "`", "`");
    }

    @Override
    protected String connectorType() {
        return "mysql";
    }

    @Override
    protected void createSchema(final JdbcConnection db, final String schemaName) throws SQLException {
        db.execute(
                "DROP DATABASE IF EXISTS " + schemaName,
                "CREATE DATABASE " + schemaName,
                String.format("GRANT ALL PRIVILEGES ON %s.* TO 'mysqluser'@'%%';", schemaName));
    }
}

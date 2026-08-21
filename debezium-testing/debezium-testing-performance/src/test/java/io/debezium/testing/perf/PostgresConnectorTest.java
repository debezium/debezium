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
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import io.debezium.junit.SkipLongRunning;

@SkipLongRunning
public class PostgresConnectorTest extends AbstractPerformanceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresConnectorTest.class);

    @Override
    protected JdbcConnection databaseConnection() {
        final ConnectionFactory connectionFactory = JdbcConnection.patternBasedFactory(
                "jdbc:postgresql://${" + JdbcConfiguration.HOSTNAME + "}:${"
                        + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}");
        final Configuration config = config().databaseConfiguration().edit()
                .withDefault("hostname", "localhost")
                .withDefault("port", "5432")
                .withDefault("user", "postgres")
                .withDefault("password", "postgres")
                .withDefault("dbname", "postgres")
                .build();
        return new JdbcConnection(config, connectionFactory, "\"", "\"");
    }

    @Override
    protected String connectorType() {
        return "postgres";
    }

    @Override
    protected void createSchema(final JdbcConnection db, final String schemaName) throws SQLException {
        db.execute(
                "DROP SCHEMA IF EXISTS " + schemaName + " CASCADE",
                "CREATE SCHEMA " + schemaName);
    }
}

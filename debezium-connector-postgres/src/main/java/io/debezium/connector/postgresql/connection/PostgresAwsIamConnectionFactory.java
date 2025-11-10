/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.sql.Connection;
import java.sql.SQLException;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * Connection factory for AWS IAM Authentication
 *
 * @author Bertrand Paquet
 */
public class PostgresAwsIamConnectionFactory implements JdbcConnection.ConnectionFactory {
    private static final String AWS_IAM_URL_PATTERN = "jdbc:aws-wrapper:postgresql://${" + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}?wrapperPlugins=iam";

    public static final String AWS_JDBC_WRAPPER_CLASS = "software.amazon.jdbc.Driver";

    private static final JdbcConnection.ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(
            AWS_IAM_URL_PATTERN,
            AWS_JDBC_WRAPPER_CLASS,
            PostgresConnection.class.getClassLoader(),
            JdbcConfiguration.PORT.withDefault(PostgresConnectorConfig.PORT.defaultValueAsString()));

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        return FACTORY.connect(config);
    }
}

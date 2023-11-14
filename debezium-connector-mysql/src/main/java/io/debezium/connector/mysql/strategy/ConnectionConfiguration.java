/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

/**
 * @author Chris Cranford
 */
public interface ConnectionConfiguration {
    JdbcConfiguration config();

    Configuration originalConfig();

    JdbcConnection.ConnectionFactory factory();

    String username();

    String password();

    String hostname();

    int port();

    MySqlConnectorConfig.SecureConnectionMode sslMode();

    boolean sslModeEnabled();

    String sslKeyStore();

    char[] sslKeyStorePassword();

    String sslTrustStore();

    char[] sslTrustStorePassword();
}

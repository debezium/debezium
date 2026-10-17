/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.jdbc;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig.SecureConnectionMode;
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

    SecureConnectionMode sslMode();

    boolean sslModeEnabled();

    String sslKeyStore();

    char[] sslKeyStorePassword();

    String sslTrustStore();

    char[] sslTrustStorePassword();

    String getUrlPattern();

    /**
     * Returns the time zone that the JDBC driver is configured to assume for the server, such as {@code UTC} or
     * {@code Asia/Seoul}, or a value that lets the driver determine it, such as {@code SERVER} or {@code auto}.
     *
     * @return the configured time zone; may be null
     */
    String connectionTimeZone();
}

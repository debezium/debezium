/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb.jdbc;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.jdbc.BinlogConnectionConfiguration;
import io.debezium.connector.mariadb.MariaDbConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
public class MariaDbConnectionConfiguration extends BinlogConnectionConfiguration {

    private static final String JDBC_PROPERTY_MARIADB_TIME_ZONE = "timezone";
    private static final String URL_PATTERN = "jdbc:mariadb://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";

    public MariaDbConnectionConfiguration(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected String getConnectionTimeZonePropertyName() {
        return JDBC_PROPERTY_MARIADB_TIME_ZONE;
    }

    @Override
    protected String resolveConnectionTimeZone(Configuration configuration) {
        // Debezium expects timezone data delivered in server timezone by default.
        return Strings.defaultIfBlank(configuration.getString(JDBC_PROPERTY_MARIADB_TIME_ZONE), "auto");
    }

    @Override
    protected JdbcConnection.ConnectionFactory createFactory(Configuration configuration) {
        return JdbcConnection.patternBasedFactory(URL_PATTERN);
    }

    @Override
    public String getUrlPattern() {
        return URL_PATTERN;
    }

    @Override
    public BinlogConnectorConfig.SecureConnectionMode sslMode() {
        final String sslMode = originalConfig().getString(MariaDbConnectorConfig.SSL_MODE);
        return MariaDbConnectorConfig.MariaDbSecureConnectionMode.parse(sslMode);
    }

    @Override
    public boolean sslModeEnabled() {
        return sslMode() != MariaDbConnectorConfig.MariaDbSecureConnectionMode.DISABLE;
    }
}

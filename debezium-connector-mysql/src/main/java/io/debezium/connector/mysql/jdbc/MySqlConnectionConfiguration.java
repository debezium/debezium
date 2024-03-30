/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.jdbc;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.jdbc.BinlogConnectionConfiguration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Strings;

/**
 * An {@link BinlogConnectionConfiguration} implementation for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlConnectionConfiguration extends BinlogConnectionConfiguration {

    private static final String JDBC_PROPERTY_CONNECTION_TIME_ZONE = "connectionTimeZone";
    public static final String URL_PATTERN = "${protocol}://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&connectTimeout=${connectTimeout}";

    public MySqlConnectionConfiguration(Configuration config) {
        super(config);
    }

    @Override
    protected String getConnectionTimeZonePropertyName() {
        return JDBC_PROPERTY_CONNECTION_TIME_ZONE;
    }

    @Override
    protected String resolveConnectionTimeZone(Configuration dbConfig) {
        // Debezium by default expects time zoned data delivered in server timezone
        return Strings.defaultIfBlank(dbConfig.getString(JDBC_PROPERTY_CONNECTION_TIME_ZONE), "SERVER");
    }

    @Override
    public String getUrlPattern() {
        return URL_PATTERN;
    }

    @Override
    protected JdbcConnection.ConnectionFactory createFactory(Configuration configuration) {
        final String driverClassName = configuration.getString(MySqlConnectorConfig.JDBC_DRIVER);
        return JdbcConnection.patternBasedFactory(
                URL_PATTERN,
                driverClassName,
                getClass().getClassLoader(),
                MySqlConnectorConfig.JDBC_PROTOCOL);
    }
}

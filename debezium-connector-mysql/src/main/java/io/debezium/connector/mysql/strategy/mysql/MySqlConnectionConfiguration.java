/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mysql;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.strategy.AbstractConnectionConfiguration;

/**
 * An {@link AbstractConnectionConfiguration} implementation for MySQL.
 *
 * @author Chris Cranford
 */
public class MySqlConnectionConfiguration extends AbstractConnectionConfiguration {

    private static final String JDBC_PROPERTY_CONNECTION_TIME_ZONE = "connectionTimeZone";

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
        String connectionTimeZone = dbConfig.getString(JDBC_PROPERTY_CONNECTION_TIME_ZONE);
        return connectionTimeZone != null ? connectionTimeZone : "SERVER";
        // return !Strings.isNullOrBlank(connectionTimeZone) ? connectionTimeZone : "SERVER";
    }

}

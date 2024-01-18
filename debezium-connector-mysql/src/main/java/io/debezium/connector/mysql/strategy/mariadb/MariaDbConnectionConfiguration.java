/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mariadb;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.strategy.AbstractConnectionConfiguration;
import io.debezium.util.Strings;

/**
 * An {@link AbstractConnectionConfiguration} for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbConnectionConfiguration extends AbstractConnectionConfiguration {

    private static final String JDBC_PROPERTY_MARIADB_TIME_ZONE = "timezone";

    public MariaDbConnectionConfiguration(Configuration config) {
        super(config);
    }

    @Override
    protected String getConnectionTimeZonePropertyName() {
        return JDBC_PROPERTY_MARIADB_TIME_ZONE;
    }

    @Override
    protected String resolveConnectionTimeZone(Configuration dbConfig) {
        // Debezium by default expected timezone data delivered in server timezone
        String timezone = dbConfig.getString(JDBC_PROPERTY_MARIADB_TIME_ZONE);
        return !Strings.isNullOrBlank(timezone) ? timezone : "auto";
    }

}

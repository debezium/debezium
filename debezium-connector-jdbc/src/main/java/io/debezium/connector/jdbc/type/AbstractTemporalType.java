/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.time.ZoneId;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;

/**
 * An abstract base class for all temporal implementations of {@link Type}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTemporalType extends AbstractType {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTemporalType.class);

    private TimeZone databaseTimeZone;

    @Override
    public void configure(JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
        super.configure(config, dialect);

        final String databaseTimeZone = config.getDatabaseTimeZone();
        try {
            this.databaseTimeZone = TimeZone.getTimeZone(ZoneId.of(databaseTimeZone));
        }
        catch (Exception e) {
            LOGGER.error("Failed to resolve time zone '{}', please specify a correct time zone value", databaseTimeZone, e);
            throw e;
        }
    }

    protected TimeZone getDatabaseTimeZone() {
        return databaseTimeZone;
    }

}

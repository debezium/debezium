/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.time.ZoneId;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalPrecisionLossHandlingMode;
import io.debezium.connector.jdbc.JdbcSinkConnectorConfig.TemporalRangeLossHandlingMode;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.sink.SinkConnectorConfig;
import io.debezium.sink.column.ColumnDescriptor;

/**
 * An abstract base class for all temporal implementations of {@link JdbcType}.
 *
 * @author Chris Cranford
 */
public abstract class AbstractTemporalType extends AbstractType {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTemporalType.class);

    private TimeZone databaseTimeZone;
    private TemporalPrecisionLossHandlingMode precisionLossHandlingMode = TemporalPrecisionLossHandlingMode.FAIL;
    private TemporalRangeLossHandlingMode rangeLossHandlingMode = TemporalRangeLossHandlingMode.FAIL;

    @Override
    public void configure(SinkConnectorConfig config, DatabaseDialect dialect) {
        super.configure(config, dialect);

        final String databaseTimeZone = config.useTimeZone();
        try {
            this.databaseTimeZone = TimeZone.getTimeZone(ZoneId.of(databaseTimeZone));
        }
        catch (Exception e) {
            LOGGER.error("Failed to resolve time zone '{}', please specify a correct time zone value", databaseTimeZone, e);
            throw e;
        }

        if (config instanceof JdbcSinkConnectorConfig jdbcConfig && jdbcConfig.getTemporalPrecisionLossHandlingMode() != null) {
            precisionLossHandlingMode = jdbcConfig.getTemporalPrecisionLossHandlingMode();
        }
        if (config instanceof JdbcSinkConnectorConfig jdbcConfig && jdbcConfig.getTemporalRangeLossHandlingMode() != null) {
            rangeLossHandlingMode = jdbcConfig.getTemporalRangeLossHandlingMode();
        }
    }

    protected TimeZone getDatabaseTimeZone() {
        return databaseTimeZone;
    }

    protected TemporalPrecisionLossHandlingMode getPrecisionLossHandlingMode() {
        return precisionLossHandlingMode;
    }

    protected TemporalRangeLossHandlingMode getRangeLossHandlingMode() {
        return rangeLossHandlingMode;
    }

    protected String targetDescription(Schema schema) {
        return String.format("target type '%s'", getTypeName(schema, false));
    }

    protected String targetDescription(ColumnDescriptor column) {
        return String.format("target column '%s' (%s)", column.getColumnName(), column.getTypeName());
    }

}

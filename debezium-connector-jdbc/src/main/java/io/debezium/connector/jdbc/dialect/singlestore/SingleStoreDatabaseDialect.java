/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import java.util.EnumSet;

import org.hibernate.SessionFactory;
import org.hibernate.community.dialect.SingleStoreDialect;
import org.hibernate.dialect.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.mysql.MariaDbDatabaseDialect;
import io.debezium.connector.jdbc.type.debezium.TargetTemporalCapabilities;
import io.debezium.connector.jdbc.type.debezium.TemporalRange;
import io.debezium.connector.jdbc.type.debezium.TemporalRange.Boundary;
import io.debezium.time.StructuredDuration;

/**
 * A {@link DatabaseDialect} implementation for SingleStore.
 */
public class SingleStoreDatabaseDialect extends MariaDbDatabaseDialect {

    private static final TemporalRange DATETIME_RANGE = new TemporalRange(
            Boundary.timestamp(1000, 1, 1, 0, 0, 0, 0),
            Boundary.timestamp(9999, 12, 31, 23, 59, 59, 999_999_000_000L));

    private static final TemporalRange TIMESTAMP_RANGE = new TemporalRange(
            Boundary.timestamp(1970, 1, 1, 0, 0, 1, 0),
            Boundary.timestamp(2038, 1, 19, 3, 14, 7, 999_999_000_000L));

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleStoreDatabaseDialect.class);

    public static class SingleStoreDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof SingleStoreDialect;
        }

        @Override
        public Class<?> name() {
            return SingleStoreDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            LOGGER.info("SingleStore Dialect instantiated.");
            return new SingleStoreDatabaseDialect(config, sessionFactory);
        }
    }

    private SingleStoreDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    @Override
    public TargetTemporalCapabilities getTargetTemporalCapabilities() {
        return TargetTemporalCapabilities.defaults(getMaxTimePrecision(), getMaxTimestampPrecision())
                .withDateRange(TemporalRange.dateYears(1000, 9999))
                .withTimestampRange(DATETIME_RANGE)
                .withTimestampRangeForType(TIMESTAMP_RANGE, "timestamp")
                .withDurationKinds(EnumSet.of(StructuredDuration.Kind.ELAPSED_TIME));
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();

        registerType(JsonType.INSTANCE);
        registerType(MapToJsonType.INSTANCE);
        registerType(GeometryType.INSTANCE);
        registerType(PointType.INSTANCE);
        registerType(FloatVectorType.INSTANCE);
        registerType(DoubleVectorType.INSTANCE);
        registerType(SingleStoreStructuredDurationType.INSTANCE);
    }
}

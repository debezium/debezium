/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.singlestore;

import org.hibernate.SessionFactory;
import org.hibernate.community.dialect.SingleStoreDialect;
import org.hibernate.dialect.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.mysql.MariaDbDatabaseDialect;

/**
 * A {@link DatabaseDialect} implementation for SingleStore.
 */
public class SingleStoreDatabaseDialect extends MariaDbDatabaseDialect {

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
    protected void registerTypes() {
        super.registerTypes();

        registerType(JsonType.INSTANCE);
        registerType(MapToJsonType.INSTANCE);
        registerType(GeometryType.INSTANCE);
        registerType(PointType.INSTANCE);
        registerType(FloatVectorType.INSTANCE);
        registerType(DoubleVectorType.INSTANCE);
    }
}

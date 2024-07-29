/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect;

import java.util.ServiceLoader;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;

/**
 * Resolves which {@link DatabaseDialect} should be used by the JDBC sink connector.
 *
 * @author Chris Cranford
 */
public class DatabaseDialectResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseDialectResolver.class);

    /**
     * Resolve the database dialect to be used.
     *
     * If no provider is found that supports the underlying Hibernate dialect, the resolver
     * will automatically return an instance of {@link GeneralDatabaseDialect}.
     *
     * @param config the sink connector's configuration, should never be {@code null}
     * @param sessionFactory hibernate session factory, should never be {@code null}
     * @return the database dialect to be used, never {@code null}
     */
    public static DatabaseDialect resolve(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        final SessionFactoryImplementor implementor = sessionFactory.unwrap(SessionFactoryImplementor.class);
        final Dialect dialect = implementor.getJdbcServices().getDialect();

        final ServiceLoader<DatabaseDialectProvider> providers = ServiceLoader.load(DatabaseDialectProvider.class);
        for (DatabaseDialectProvider provider : providers) {
            if (provider.supports(dialect)) {
                LOGGER.info("Using dialect {}", provider.name().getName());
                return provider.instantiate(config, sessionFactory);
            }
        }

        LOGGER.info("Using dialect {}", GeneralDatabaseDialect.class.getName());
        return new GeneralDatabaseDialect(config, sessionFactory);
    }
}

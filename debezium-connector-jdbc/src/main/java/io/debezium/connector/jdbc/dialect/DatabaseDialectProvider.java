/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;

/**
 * A provider for a {@link DatabaseDialect}.
 *
 * @author Chris Cranford
 */
public interface DatabaseDialectProvider {
    /**
     * Returns whether this provider supports the provided Hibernate dialect.
     *
     * @param dialect hibernate dialect, should not be {@code null}
     * @return true if the provider supports the dialect, otherwise false
     */
    boolean supports(Dialect dialect);

    /**
     * Returns the class that will be instantiated.
     */
    Class<?> name();

    /**
     * Instantiates the underlying database dialect implementation.
     *
     * @param config the sink connector configuration, should not be {@code nul}
     * @param sessionFactory hibernate session factory, should not be {@code null}
     * @return the created database dialect instance, never {@code null}
     */
    DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory);
}

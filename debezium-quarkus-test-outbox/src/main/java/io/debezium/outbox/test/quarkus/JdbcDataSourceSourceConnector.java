/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.outbox.test.quarkus;

import java.util.Arrays;
import java.util.Objects;

/**
 * Enum representing supported JDBC data source connectors for Debezium.
 *
 * <p>
 * This enum defines the JDBC connectors supported by Debezium, starting with
 * PostgreSQL, and provides the necessary methods to retrieve the connector class
 * that Debezium should use for CDC (Change Data Capture).
 * </p>
 *
 * <p>
 * In the context of the **Debezium Outbox pattern**, this enum helps specify the
 * database connector type and ensures that the proper connector is chosen based
 * on the underlying database kind.
 * </p>
 */
public enum JdbcDataSourceSourceConnector {
    /**
     * PostgreSQL connector configuration.
     *
     * <p>
     * This constant is used when the database being used is PostgreSQL. It defines
     * both the database kind (used to identify the database type) and the Debezium
     * connector class that will handle the Change Data Capture (CDC) for this type
     * of database.
     * </p>
     */
    POSTGRESQL {
        @Override
        public String dbKind() {
            return "postgresql";
        }

        @Override
        public String connectorClass() {
            return "io.debezium.connector.postgresql.PostgresConnector";
        }
    };

    /**
     * Returns the kind of the database that this connector works with.
     *
     * @return the string representing the database kind (e.g., "postgresql")
     */
    public abstract String dbKind();

    /**
     * Returns the Debezium connector class to be used for the specific database kind.
     *
     * <p>
     * This is used to inform Debezium of the appropriate connector class that will
     * handle Change Data Capture (CDC) for the given database type. For instance,
     * for PostgreSQL, the connector class would be
     * "io.debezium.connector.postgresql.PostgresConnector".
     * </p>
     *
     * @return the fully qualified class name of the Debezium connector.
     */
    public abstract String connectorClass();

    /**
     * Retrieves the corresponding `JdbcDataSourceSourceConnector` from a given database kind.
     *
     * <p>
     * This method is essential for dynamically determining the appropriate
     * connector based on the database type specified in the configuration. If
     * an unsupported database kind is provided, an exception is thrown.
     * </p>
     *
     * @param dbKind the string representing the kind of the database (e.g., "postgresql")
     * @return the matching JdbcDataSourceSourceConnector instance
     * @throws IllegalArgumentException if the provided database kind is unsupported
     */
    public static JdbcDataSourceSourceConnector fromDbKind(final String dbKind) {
        Objects.requireNonNull(dbKind);
        return Arrays.stream(JdbcDataSourceSourceConnector.values())
                .filter(jdbcDataSourceSourceConnector -> dbKind.equals(jdbcDataSourceSourceConnector.dbKind()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported database kind: " + dbKind));
    }

}

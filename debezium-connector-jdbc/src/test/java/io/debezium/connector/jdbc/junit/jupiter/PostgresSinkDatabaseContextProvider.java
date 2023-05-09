/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter;

import java.lang.reflect.Method;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import io.debezium.connector.jdbc.junit.PostgresExtensionUtils;
import io.debezium.connector.jdbc.junit.TestHelper;

/**
 * An implementation of {@link AbstractSinkDatabaseContextProvider} for PostgreSQL.
 *
 * @author Chris Cranford
 */
public class PostgresSinkDatabaseContextProvider extends AbstractSinkDatabaseContextProvider implements BeforeEachCallback, AfterEachCallback {

    // We explicitly use debezium/postgres which has the POSTGIS extension available.
    // The standard postgres image does not ship with POSTGIS available by default.
    private static final DockerImageName IMAGE_NAME = DockerImageName.parse("debezium/postgres")
            .asCompatibleSubstituteFor("postgres");

    @SuppressWarnings("resource")
    public PostgresSinkDatabaseContextProvider() {
        super(SinkType.POSTGRES,
                new PostgreSQLContainer<>(IMAGE_NAME)
                        .withNetwork(Network.newNetwork())
                        .withDatabaseName("test")
                        .withEnv("TZ", TestHelper.getSinkTimeZone())
                        .withEnv("PGTZ", TestHelper.getSinkTimeZone()));
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        final Method method = context.getRequiredTestMethod();
        final WithPostgresExtension postgresExtension = method.getAnnotation(WithPostgresExtension.class);
        if (postgresExtension != null) {
            PostgresExtensionUtils.createExtension(getSink(), postgresExtension.value());
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        final Method method = context.getRequiredTestMethod();
        final WithPostgresExtension postgresExtension = method.getAnnotation(WithPostgresExtension.class);
        if (postgresExtension != null) {
            PostgresExtensionUtils.dropExtension(getSink(), postgresExtension.value());
        }
    }

}

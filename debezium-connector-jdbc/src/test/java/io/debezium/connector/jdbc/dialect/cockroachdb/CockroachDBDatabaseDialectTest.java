/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ServiceLoader;

import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.TransactionSerializationException;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.postgres.PostgresDatabaseDialect;

/**
 * Unit tests for {@link CockroachDBDatabaseDialect}'s provider resolution. CockroachDB maps to
 * Hibernate's {@link CockroachDialect}, which is not a {@code PostgreSQLDialect}, so without a
 * dedicated provider the sink falls back to the general dialect (no upsert support).
 *
 * @author Virag Tripathi
 */
public class CockroachDBDatabaseDialectTest {

    private final CockroachDBDatabaseDialect.CockroachDBDatabaseDialectProvider provider = new CockroachDBDatabaseDialect.CockroachDBDatabaseDialectProvider();

    @Test
    void providerSupportsCockroachDialect() {
        assertThat(provider.supports(new CockroachDialect()))
                .as("CockroachDB provider should support Hibernate's CockroachDialect")
                .isTrue();
    }

    @Test
    void providerDoesNotClaimPlainPostgres() {
        assertThat(provider.supports(new PostgreSQLDialect()))
                .as("CockroachDB provider should not claim a plain PostgreSQL dialect")
                .isFalse();
    }

    @Test
    void providerNameIsCockroachDbDialect() {
        assertThat(provider.name()).isEqualTo(CockroachDBDatabaseDialect.class);
    }

    @Test
    void postgresProviderDoesNotClaimCockroach() {
        // This is the gap dbz#1639 closes: the Postgres provider only matches PostgreSQLDialect, so
        // CockroachDialect would otherwise fall through to the general dialect (no upsert).
        assertThat(new PostgresDatabaseDialect.PostgresDatabaseDialectProvider().supports(new CockroachDialect()))
                .as("Postgres provider must not match CockroachDialect")
                .isFalse();
    }

    @Test
    void providerIsRegisteredViaServiceLoader() {
        boolean found = false;
        for (DatabaseDialectProvider p : ServiceLoader.load(DatabaseDialectProvider.class)) {
            if (p instanceof CockroachDBDatabaseDialect.CockroachDBDatabaseDialectProvider) {
                found = true;
                break;
            }
        }
        assertThat(found)
                .as("CockroachDB provider should be registered in META-INF/services")
                .isTrue();
    }

    @Test
    void retriableSetCoversSerializationAndDeadlockConflicts() {
        // CockroachDB SQLSTATE 40001 -> TransactionSerializationException and 40P01 -> LockAcquisitionException.
        // They are siblings under JDBCException (40001 is NOT a LockAcquisitionException), so both must be
        // declared retriable for the sink's isRetriable() to cover serialization failures and deadlocks.
        assertThat(CockroachDBDatabaseDialect.RETRIABLE_CONFLICT_EXCEPTIONS)
                .contains(TransactionSerializationException.class, LockAcquisitionException.class);
        assertThat(LockAcquisitionException.class.isAssignableFrom(TransactionSerializationException.class))
                .as("40001 is a sibling of LockAcquisitionException, not a subclass")
                .isFalse();
    }

    @Test
    void exactlyOneRegisteredProviderSupportsCockroach() {
        CockroachDialect dialect = new CockroachDialect();
        long matches = 0;
        Class<?> matchName = null;
        for (DatabaseDialectProvider p : ServiceLoader.load(DatabaseDialectProvider.class)) {
            if (p.supports(dialect)) {
                matches++;
                matchName = p.name();
            }
        }
        assertThat(matches)
                .as("Exactly one provider should resolve CockroachDialect")
                .isEqualTo(1);
        assertThat(matchName)
                .as("The resolving provider should be the CockroachDB dialect")
                .isEqualTo(CockroachDBDatabaseDialect.class);
    }
}

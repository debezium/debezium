/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.cockroachdb;

import java.sql.Types;
import java.util.Set;

import org.hibernate.SessionFactory;
import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.TransactionSerializationException;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.postgres.PostgresDatabaseDialect;

/**
 * A {@link DatabaseDialect} implementation for CockroachDB.
 *
 * <p>CockroachDB speaks the PostgreSQL wire protocol and supports {@code INSERT ... ON CONFLICT},
 * so it reuses {@link PostgresDatabaseDialect}. The distinct dialect exists because Hibernate maps
 * CockroachDB to {@link CockroachDialect}, which is not a {@code PostgreSQLDialect}; without a
 * matching provider the resolver falls back to the general dialect, which does not support upsert.</p>
 *
 * @author Virag Tripathi
 */
public class CockroachDBDatabaseDialect extends PostgresDatabaseDialect {

    public static class CockroachDBDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof CockroachDialect;
        }

        @Override
        public Class<?> name() {
            return CockroachDBDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new CockroachDBDatabaseDialect(config, sessionFactory);
        }
    }

    protected CockroachDBDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    /**
     * Transient transaction-conflict exceptions CockroachDB raises that the client is expected to
     * retry. CockroachDB runs SERIALIZABLE isolation by default and signals conflicts with SQLSTATE
     * 40001 (serialization failure) and 40P01 (deadlock); Hibernate maps these to
     * {@link TransactionSerializationException} and {@link LockAcquisitionException} respectively.
     * The two are siblings under {@code JDBCException}, so both must be listed.
     */
    static final Set<Class<? extends Exception>> RETRIABLE_CONFLICT_EXCEPTIONS = Set.of(
            TransactionSerializationException.class,
            LockAcquisitionException.class);

    @Override
    public Set<Class<? extends Exception>> getCommunicationExceptions() {
        // Add CockroachDB's retriable transaction-conflict exceptions so the configured flush retries
        // (flush.max.retries / flush.retry.delay.ms) handle them rather than failing the task.
        final Set<Class<? extends Exception>> exceptions = super.getCommunicationExceptions();
        exceptions.addAll(RETRIABLE_CONFLICT_EXCEPTIONS);
        return exceptions;
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();
        // Override inherited PostgreSQL type mappings that CockroachDB does not support, verified
        // against CockroachDB v25.4 and v26.3: xml, ranges, macaddr, and cidr are stored as text;
        // MAP is stored as jsonb rather than hstore; money as a fixed-scale decimal; the spatial
        // types use CockroachDB's built-in geometry/geography without a PostGIS schema; float
        // vectors use the native vector type since halfvec does not exist; sparse vectors are
        // unsupported.
        registerType(TextFallbackType.INSTANCE);
        registerType(JsonType.INSTANCE);
        registerType(MapToJsonbType.INSTANCE);
        registerType(MoneyType.INSTANCE);
        registerType(GeometryType.INSTANCE);
        registerType(GeographyType.INSTANCE);
        registerType(PointType.INSTANCE);
        registerType(FloatVectorType.INSTANCE);
        registerType(SparseDoubleVectorType.INSTANCE);
    }

    @Override
    public String getJdbcTypeName(int jdbcType) {
        // The Hibernate CockroachDialect names character types "string", a CockroachDB alias that is
        // valid in DDL but absent from pg_type, so the PgJDBC driver cannot resolve it as an array
        // element type in Connection#createArrayOf. Use the PostgreSQL-compatible name instead, which
        // CockroachDB supports in both DDL and pg_type.
        return switch (jdbcType) {
            case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR -> "text";
            default -> super.getJdbcTypeName(jdbcType);
        };
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.pipeline.txmetadata.DefaultTransactionStructMaker;

/**
 * Verifies that {@link PostgresTransactionStructMaker} adds the optional {@code commit_lsn} field to
 * the Postgres transaction metadata schemas, and that the shared/core schema is left untouched.
 */
public class PostgresTransactionStructMakerTest {

    private final DefaultTransactionStructMaker defaultMaker = new DefaultTransactionStructMaker(Configuration.empty());
    private final PostgresTransactionStructMaker postgresMaker = new PostgresTransactionStructMaker(Configuration.empty());

    @Test
    void transactionValueSchemaAddsOptionalCommitLsn() {
        final Schema base = defaultMaker.getTransactionValueSchema();
        final Schema pg = postgresMaker.getTransactionValueSchema();

        assertThat(pg.field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY)).isNotNull();
        assertThat(pg.field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY).schema())
                .isEqualTo(Schema.OPTIONAL_INT64_SCHEMA);
        // Schema name is preserved so consumers keying off it still recognise the record.
        assertThat(pg.name()).isEqualTo(base.name());
        assertThat(pg.version()).isEqualTo(2);
    }

    @Test
    void transactionBlockSchemaAddsOptionalCommitLsn() {
        final Schema base = defaultMaker.getTransactionBlockSchema();
        final Schema pg = postgresMaker.getTransactionBlockSchema();

        assertThat(pg.field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY)).isNotNull();
        assertThat(pg.field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY).schema())
                .isEqualTo(Schema.OPTIONAL_INT64_SCHEMA);
        assertThat(pg.name()).isEqualTo(base.name());
        assertThat(pg.version()).isEqualTo(2);
    }

    @Test
    void sharedSchemaIsUnaffected() {
        // commit_lsn must be Postgres-only: the shared/core transaction schema must not gain the
        // field, otherwise every other connector's transaction metadata (and tests) would change.
        assertThat(defaultMaker.getTransactionValueSchema()
                .field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY)).isNull();
        assertThat(defaultMaker.getTransactionBlockSchema()
                .field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY)).isNull();
    }
}

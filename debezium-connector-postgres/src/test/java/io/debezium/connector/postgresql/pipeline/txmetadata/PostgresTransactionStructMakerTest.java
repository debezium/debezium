/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.pipeline.txmetadata.DefaultTransactionStructMaker;

/**
 * Verifies that {@link PostgresTransactionStructMaker} augments the transaction metadata schemas
 * with an optional {@code commit_lsn} field while preserving every field of the base schema (name,
 * type and optionality), and that the shared/core schema is left untouched.
 */
public class PostgresTransactionStructMakerTest {

    private final DefaultTransactionStructMaker defaultMaker = new DefaultTransactionStructMaker(Configuration.empty());
    private final PostgresTransactionStructMaker postgresMaker = new PostgresTransactionStructMaker(Configuration.empty());

    @Test
    @FixFor("debezium/dbz#2353")
    void transactionValueSchemaAddsOptionalCommitLsn() {
        assertSchemaAugmentedWithCommitLsn(defaultMaker.getTransactionValueSchema(), postgresMaker.getTransactionValueSchema());
    }

    @Test
    @FixFor("debezium/dbz#2353")
    void transactionBlockSchemaAddsOptionalCommitLsn() {
        assertSchemaAugmentedWithCommitLsn(defaultMaker.getTransactionBlockSchema(), postgresMaker.getTransactionBlockSchema());
    }

    @Test
    @FixFor("debezium/dbz#2353")
    void sharedSchemaIsUnaffected() {
        // commit_lsn must be Postgres-only: the shared/core transaction schema must not gain the
        // field, otherwise every other connector's transaction metadata (and tests) would change.
        assertThat(defaultMaker.getTransactionValueSchema()
                .field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY)).isNull();
        assertThat(defaultMaker.getTransactionBlockSchema()
                .field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY)).isNull();
    }

    /**
     * Asserts that the Postgres schema keeps the base schema's name, type and optionality, preserves
     * every base field (name + type), and appends the optional {@code commit_lsn} field with a bumped
     * schema version.
     */
    private static void assertSchemaAugmentedWithCommitLsn(Schema base, Schema pg) {
        // Structure carried over from the base schema.
        assertThat(pg.type()).isEqualTo(base.type());
        assertThat(pg.name()).isEqualTo(base.name());
        assertThat(pg.isOptional()).isEqualTo(base.isOptional());
        assertThat(pg.version()).isEqualTo(2);

        // Every base field is preserved with the same field schema, in order.
        for (Field baseField : base.fields()) {
            final Field pgField = pg.field(baseField.name());
            assertThat(pgField).as("missing field %s", baseField.name()).isNotNull();
            assertThat(pgField.schema()).as("field %s schema changed", baseField.name()).isEqualTo(baseField.schema());
        }

        // The commit_lsn field is appended as an optional INT64 and is the only added field.
        final Field commitLsn = pg.field(PostgresTransactionStructMaker.DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY);
        assertThat(commitLsn).isNotNull();
        assertThat(commitLsn.schema()).isEqualTo(Schema.OPTIONAL_INT64_SCHEMA);
        assertThat(pg.fields()).hasSize(base.fields().size() + 1);
    }
}

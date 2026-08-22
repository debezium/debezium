/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.pipeline.txmetadata;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.Configuration;
import io.debezium.pipeline.txmetadata.DefaultTransactionStructMaker;

/**
 * Postgres-specific {@link io.debezium.pipeline.txmetadata.TransactionStructMaker} that augments the
 * shared transaction-metadata schemas with an optional {@code commit_lsn} field.
 *
 * <p>The value is the transaction's commit LSN (pgoutput {@code Begin.final_lsn}): constant across
 * every event of a transaction and strictly increasing in commit order, which makes it usable as a
 * single-scalar ordering/dedup watermark downstream (unlike the per-row {@code source.lsn}).
 *
 * <p>The field is added here, in the Postgres connector, rather than in the shared/core transaction
 * schema, so that no other connector's transaction metadata (or tests) are affected. The schema
 * names are preserved and only the version is bumped, so consumers keying off the schema name
 * continue to recognise the record.
 */
public class PostgresTransactionStructMaker extends DefaultTransactionStructMaker {

    public static final String DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY = "commit_lsn";

    // Version of the Postgres transaction schemas. Kept independent of the core transaction schema
    // version so a future change to the core schema does not implicitly change this one.
    private static final int POSTGRES_TRANSACTION_SCHEMA_VERSION = 2;

    private final Schema postgresTransactionBlockSchema;
    private final Schema postgresTransactionValueSchema;

    public PostgresTransactionStructMaker(Configuration config) {
        super(config);
        this.postgresTransactionBlockSchema = withCommitLsn(super.getTransactionBlockSchema());
        this.postgresTransactionValueSchema = withCommitLsn(super.getTransactionValueSchema());
    }

    @Override
    public Schema getTransactionBlockSchema() {
        return postgresTransactionBlockSchema;
    }

    @Override
    public Schema getTransactionValueSchema() {
        return postgresTransactionValueSchema;
    }

    /**
     * Returns a copy of {@code base} with the optional {@code commit_lsn} field appended. The
     * schema name is preserved; the version is an explicit Postgres-owned constant so it does not
     * track the core transaction schema version.
     */
    private static Schema withCommitLsn(Schema base) {
        final SchemaBuilder builder = SchemaBuilder.struct()
                .name(base.name())
                .version(POSTGRES_TRANSACTION_SCHEMA_VERSION);
        for (Field field : base.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY, Schema.OPTIONAL_INT64_SCHEMA);
        return builder.build();
    }
}

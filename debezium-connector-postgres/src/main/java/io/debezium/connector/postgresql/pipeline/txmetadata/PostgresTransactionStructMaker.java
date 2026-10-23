/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.pipeline.txmetadata;

import static io.debezium.config.CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE;

import org.apache.kafka.connect.data.Schema;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresSchemaFactory;
import io.debezium.pipeline.txmetadata.DefaultTransactionStructMaker;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Postgres-specific {@link io.debezium.pipeline.txmetadata.TransactionStructMaker} that augments the
 * transaction-metadata schemas with an optional {@code commit_lsn} field.
 *
 * <p>The value is the transaction's commit LSN (pgoutput {@code Begin.final_lsn}): constant across
 * every event of a transaction and strictly increasing in commit order, which makes it usable as a
 * single-scalar ordering/dedup watermark downstream (unlike the per-row {@code source.lsn}).
 *
 * <p>The augmented schemas are produced by {@link PostgresSchemaFactory}, so the shared/core
 * transaction schema used by other connectors is unaffected.
 */
public class PostgresTransactionStructMaker extends DefaultTransactionStructMaker {

    public static final String DEBEZIUM_TRANSACTION_COMMIT_LSN_KEY = "commit_lsn";

    private final Schema postgresTransactionBlockSchema;
    private final Schema postgresTransactionValueSchema;

    public PostgresTransactionStructMaker(Configuration config) {
        super(config);
        final SchemaNameAdjuster adjuster = CommonConnectorConfig.SchemaNameAdjustmentMode
                .parse(config.getString(SCHEMA_NAME_ADJUSTMENT_MODE)).createAdjuster();
        this.postgresTransactionBlockSchema = PostgresSchemaFactory.get().transactionBlockSchema();
        this.postgresTransactionValueSchema = PostgresSchemaFactory.get().transactionValueSchema(adjuster);
    }

    @Override
    public Schema getTransactionBlockSchema() {
        return postgresTransactionBlockSchema;
    }

    @Override
    public Schema getTransactionValueSchema() {
        return postgresTransactionValueSchema;
    }
}

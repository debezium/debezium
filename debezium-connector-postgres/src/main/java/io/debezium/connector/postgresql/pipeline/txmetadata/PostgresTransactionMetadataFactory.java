/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.pipeline.txmetadata;

import io.debezium.config.Configuration;
import io.debezium.pipeline.txmetadata.DefaultTransactionMetadataFactory;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;

/**
 * Postgres-specific transaction metadata factory that supplies a {@link PostgresTransactionStructMaker},
 * so that the Postgres transaction metadata carries the optional {@code commit_lsn} field without
 * altering the shared/core transaction schema used by other connectors.
 */
public class PostgresTransactionMetadataFactory extends DefaultTransactionMetadataFactory {

    private final Configuration configuration;

    public PostgresTransactionMetadataFactory(Configuration configuration) {
        super(configuration);
        this.configuration = configuration;
    }

    @Override
    public TransactionStructMaker getTransactionStructMaker() {
        return new PostgresTransactionStructMaker(configuration);
    }
}

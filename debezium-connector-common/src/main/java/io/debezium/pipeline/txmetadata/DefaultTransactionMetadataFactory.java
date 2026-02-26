/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import io.debezium.config.Configuration;
import io.debezium.pipeline.txmetadata.spi.TransactionMetadataFactory;

public class DefaultTransactionMetadataFactory implements TransactionMetadataFactory {

    private final Configuration configuration;

    public DefaultTransactionMetadataFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public TransactionContext getTransactionContext() {
        return new TransactionContext();
    }

    @Override
    public TransactionStructMaker getTransactionStructMaker() {
        return new DefaultTransactionStructMaker(configuration);
    }
}

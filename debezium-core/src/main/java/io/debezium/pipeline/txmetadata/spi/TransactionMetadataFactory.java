/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata.spi;

import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;

public interface TransactionMetadataFactory {
    /**
     * Provide a {@link TransactionContext} that is used for tracking/processing transaction metadata
     *
     * @return
     */
    TransactionContext getTransactionContext();

    /**
     * Provide a {@link TransactionStructMaker} that is used to build the structs that stores transaction metadata
     *
     * @return
     */
    TransactionStructMaker getTransactionStructMaker();
}

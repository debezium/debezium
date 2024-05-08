/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

/**
 * An interface for containing all metadata needed to for {@link TransactionContext} to
 * process the transaction. At least needs to provide transaction ID, can add additional
 * transaction metadata for tracking with a connector-specific subclass of {@link TransactionContext}.
 */
public interface TransactionInfo {

    /**
     * Return the string representation of the transaction ID.
     *
     * @return String of transaction ID
     */
    String getTransactionId();
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import java.util.Map;

public interface TransactionOrderMetadata {
    Map<String, Object> store(Map<String, Object> offset);

    void load(Map<String, ?> offsets);

    void beginTransaction(TransactionInfo transactionInfo);

    void endTransaction();
}

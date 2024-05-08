/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

/**
 * Describes the transition of transaction from start to end.
 *
 * @author Jiri Pechanec
 */
public enum TransactionStatus {

    BEGIN,
    END;
}

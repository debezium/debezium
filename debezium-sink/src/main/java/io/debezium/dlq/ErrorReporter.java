/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.dlq;

import io.debezium.sink.DebeziumSinkRecord;

public interface ErrorReporter {
    void report(DebeziumSinkRecord record, Exception e);
}

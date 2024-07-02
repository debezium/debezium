/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.dlq;

import org.apache.kafka.connect.sink.SinkRecord;

public interface ErrorReporter {
    void report(SinkRecord record, Exception e);
}

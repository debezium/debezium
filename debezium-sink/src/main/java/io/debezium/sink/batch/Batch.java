/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.Collection;
import java.util.LinkedHashSet;

import io.debezium.sink.DebeziumSinkRecord;

public class Batch extends LinkedHashSet<DebeziumSinkRecord> {
    public <R> Batch(R collection) {
        super((Collection<? extends DebeziumSinkRecord>) collection);
    }
}

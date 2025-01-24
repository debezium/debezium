/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import io.debezium.sink.DebeziumSinkRecord;

public class Batch extends ArrayList<DebeziumSinkRecord> {

    public Batch(Collection<? extends DebeziumSinkRecord> collection) {
        super(Collections.unmodifiableList(new ArrayList<>(collection)));
    }

}

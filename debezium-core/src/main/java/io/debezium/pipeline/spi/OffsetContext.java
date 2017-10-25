/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public interface OffsetContext {

    // load()

    Map<String, ?> getPartition();
    Map<String, ?> getOffset();
    Schema getSourceInfoSchema();
    Struct getSourceInfo();
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.pipeline.DataChangeEvent;

@FunctionalInterface
public interface ChangeEventCreator {

    DataChangeEvent createDataChangeEvent(SourceRecord sourceRecord);
}

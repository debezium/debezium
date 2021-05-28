/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import io.debezium.connector.common.SourceRecordWrapper;
import io.debezium.pipeline.DataChangeEvent;

@FunctionalInterface
public interface ChangeEventCreator {

    DataChangeEvent createDataChangeEvent(SourceRecordWrapper sourceRecord);
}

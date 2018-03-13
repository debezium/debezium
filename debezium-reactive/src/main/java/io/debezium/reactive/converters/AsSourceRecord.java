/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.converters;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.reactive.spi.AsType;

public class AsSourceRecord implements AsType<SourceRecord> {

    public Class<SourceRecord> getTargetType() {
        return SourceRecord.class;
    }
}

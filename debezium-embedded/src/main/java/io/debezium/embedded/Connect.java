/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.format.SerializationFormat;

/**
 * A {@link SerializationFormat} defining the {@link SourceRecord} type from Kafka Connect API.
 */
public class Connect implements SerializationFormat<SourceRecord> {
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import org.apache.kafka.connect.data.Schema;

import io.debezium.data.Envelope;

public interface DataCollectionSchema {

    DataCollectionId id();

    Schema keySchema();

    Envelope getEnvelopeSchema();
}

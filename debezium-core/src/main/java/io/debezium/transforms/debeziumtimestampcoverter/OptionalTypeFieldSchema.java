/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.debeziumtimestampcoverter;

import org.apache.kafka.connect.data.Schema;

/**
 * Used in creating optional fields(For non-timestamp fields) for below scenario,
 * <p>
 * Debezium message data contains all field names and types in the schema,but payload would only contain the value for the fields which are not null.
 * Inorder to avoid all of sthe validation checks that kafka imposes during transformation, optional fields are to be provided.
 * DebeziumTimestampConverter has a static block, that creates all different types of optional fields and use them whereever needed.
 */

public interface OptionalTypeFieldSchema {
    /**
     * @return the optional schema.
     */
    Schema optionalSchemaNonTimestamp();
}

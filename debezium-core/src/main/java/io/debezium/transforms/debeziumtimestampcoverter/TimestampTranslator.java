/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.transforms.debeziumtimestampcoverter;

import org.apache.kafka.connect.data.Schema;

import java.util.Date;

/**
 * DebeziumTimestampConverter uses this interface to define custom ways of converting different versions/types of timestamp/date formats.
 * DebeziumTimestampConverter has a static block that collects all the custom conversion objects into TRANSLATORS map,
 * and are called based the timestamp type conversion.
 */

public interface TimestampTranslator {
    /**
     * Convert from the type-specific format to the universal java.util.Date format
     */
    Date toRaw(DebeziumTimestampConverter.Config config, Object orig);

    /**
     * Get the schema for this format.
     */
    Schema typeSchema();

    /**
     * Convert from the universal java.util.Date format to the type-specific format
     */
    Object toType(DebeziumTimestampConverter.Config config, Date orig, String format);

    Schema optionalSchema();
}



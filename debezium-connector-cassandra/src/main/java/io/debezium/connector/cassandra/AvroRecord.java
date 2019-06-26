/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * An interface that indicates the record can be converted to a {@link GenericRecord}.
 */
public interface AvroRecord {
    /**
     * return an Avro GenericRecord based on the schema passed into the method
     * @param schema of the generic record
     * @return a GenericRecord
     */
    GenericRecord record(Schema schema);
}

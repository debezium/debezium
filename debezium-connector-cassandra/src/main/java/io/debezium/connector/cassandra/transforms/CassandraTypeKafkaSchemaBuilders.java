/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.driver.core.DataType;

import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.NanoDuration;
import io.debezium.time.Timestamp;

/**
 * Class that maps Cassandra's {@link DataType} to its corresponding Kafka Connect {@link SchemaBuilder}.
 */
public final class CassandraTypeKafkaSchemaBuilders {

    // native types
    public static final SchemaBuilder STRING_TYPE = SchemaBuilder.string().optional();
    public static final SchemaBuilder BOOLEAN_TYPE = SchemaBuilder.bool().optional();
    public static final SchemaBuilder BYTES_TYPE = SchemaBuilder.bytes().optional();
    public static final SchemaBuilder BYTE_TYPE = SchemaBuilder.int8().optional();
    public static final SchemaBuilder SHORT_TYPE = SchemaBuilder.int16().optional();
    public static final SchemaBuilder INT_TYPE = SchemaBuilder.int32().optional();
    public static final SchemaBuilder LONG_TYPE = SchemaBuilder.int64().optional();
    public static final SchemaBuilder FLOAT_TYPE = SchemaBuilder.float32().optional();
    public static final SchemaBuilder DOUBLE_TYPE = SchemaBuilder.float64().optional();

    // logical types
    public static final SchemaBuilder DATE_TYPE = Date.builder().optional();
    public static final SchemaBuilder TIMESTAMP_MILLI_TYPE = Timestamp.builder().optional();
    public static final SchemaBuilder UUID_TYPE = Uuid.builder().optional();
    public static final SchemaBuilder DURATION_TYPE = NanoDuration.builder().optional();
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import com.datastax.driver.core.DataType;
import org.apache.kafka.connect.data.SchemaBuilder;
import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.Timestamp;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.MicroDuration;


/**
 * Class that maps Cassandra's {@link DataType} to its corresponding kafka connect {@link SchemaBuilder}.
 */
public final class CassandraTypeKafkaSchemaBuilders {

    // native types
    public static final SchemaBuilder STRING_TYPE = SchemaBuilder.string();
    public static final SchemaBuilder BOOLEAN_TYPE = SchemaBuilder.bool();
    public static final SchemaBuilder BYTES_TYPE = SchemaBuilder.bytes();
    public static final SchemaBuilder BYTE_TYPE = SchemaBuilder.int8();
    public static final SchemaBuilder SHORT_TYPE = SchemaBuilder.int16();
    public static final SchemaBuilder INT_TYPE = SchemaBuilder.int32();
    public static final SchemaBuilder LONG_TYPE = SchemaBuilder.int64();
    public static final SchemaBuilder FLOAT_TYPE = SchemaBuilder.float32();
    public static final SchemaBuilder DOUBLE_TYPE = SchemaBuilder.float64();

    // logical types
    public static final SchemaBuilder DATE_TYPE = Date.builder();
    public static final SchemaBuilder TIMESTAMP_MILLI_TYPE = Timestamp.builder();
    public static final SchemaBuilder TIMESTAMP_MICRO_TYPE = MicroTimestamp.builder();
    public static final SchemaBuilder UUID_TYPE = Uuid.builder();
    public static final SchemaBuilder DURATION_TYPE = MicroDuration.builder();

}

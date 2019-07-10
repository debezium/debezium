/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import com.datastax.driver.core.DataType;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorSchemaException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.cassandra.db.marshal.AbstractType;

/**
 * Utility class that maps Cassandra's {@link DataType} to its corresponding avro {@link Schema}.
 * Used for generating schemas for {@link org.apache.avro.generic.GenericRecord} to be send to Kafka.
 */
public final class CassandraTypeToAvroSchemaMapper {

    // native types
    public static final Schema STRING_TYPE = SchemaBuilder.builder().stringType();
    public static final Schema BOOLEAN_TYPE = SchemaBuilder.builder().booleanType();
    public static final Schema BYTES_TYPE = SchemaBuilder.builder().bytesType();
    public static final Schema INT_TYPE = SchemaBuilder.builder().intType();
    public static final Schema FLOAT_TYPE = SchemaBuilder.builder().floatType();
    public static final Schema DOUBLE_TYPE = SchemaBuilder.builder().doubleType();
    public static final Schema LONG_TYPE = SchemaBuilder.builder().longType();

    // logical types
    public static final Schema DATE_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    public static final Schema TIMESTAMP_MILLI_TYPE = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    public static final Schema TIMESTAMP_MICRO_TYPE = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

    // custom types
    public static final Schema UUID_TYPE = SchemaBuilder.builder().fixed("UUID").size(16);
    public static final Schema DURATION_TYPE = SchemaBuilder.record("duration")
                                                            .fields().requiredInt("months")
                                                                     .requiredInt("days")
                                                                     .requiredInt("nanos")
                                                            .endRecord();

    private CassandraTypeToAvroSchemaMapper() { }

    public static Schema nullable(Schema schema) {
        return SchemaBuilder.builder().unionOf().nullType().and().type(schema).endUnion();
    }

    public static Schema getSchema(AbstractType<?> type, boolean nullable) {
        Schema requiredSchema;
        try {
            requiredSchema = CassandraTypeDeserializer.getSchema(type);
        } catch (NullPointerException e) {
            throw new CassandraConnectorSchemaException("Unknown data type: " + type);
        }

        if (nullable) {
            return nullable(requiredSchema);
        }
        return requiredSchema;
    }
}

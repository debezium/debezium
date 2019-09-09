/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;

public class DurationTypeDeserializer extends BasicTypeDeserializer {
    /*
     * According to the official spec, Avro has a duration type of (almost) the same format of the cassandra
     * duration type, but sadly it's not actually represented in the code anywhere!
     * issue: https://issues.apache.org/jira/browse/AVRO-2123
     * So, for now at least, duration is serialized into a record with fields months, days, and nanos.
     */

    public DurationTypeDeserializer() {
        super(CassandraTypeKafkaSchemaBuilders.DURATION_TYPE);
    }

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Duration duration = (Duration) super.deserialize(abstractType, bb);
        int months = duration.getMonths();
        int days = duration.getDays();
        long nanoSec = duration.getNanoseconds();

        Schema durationSchema = getSchemaBuilder(abstractType).build();
        return new Struct(durationSchema)
                .put("months", months)
                .put("days", days)
                .put("nanos", nanoSec);
    }
}

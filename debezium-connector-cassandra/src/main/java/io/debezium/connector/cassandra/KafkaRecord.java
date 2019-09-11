package io.debezium.connector.cassandra;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;


/**
 * An interface that indicates the record can be converted to a {@link Struct}.
 */
public interface KafkaRecord {
    /**
     * return an kafka connect Struct based on the schema passed into the method
     * @param schema of the Struct
     * @return a Struct
     */
    Struct record(Schema schema);
}

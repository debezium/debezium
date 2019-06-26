/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import io.debezium.connector.cassandra.transforms.CassandraTypeToAvroSchemaMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MapTypeDeserializer extends TypeDeserializer {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Map<?, ?> deserializedMap = (Map<?, ?>) super.deserialize(abstractType, bb);

        // we may need to do some modification to the deserialized map before returning if the key type of the map does not
        // deserialize to a string.

        MapType<?, ?> mapType = (MapType<?, ?>) abstractType;
        Schema keySchema = CassandraTypeToAvroSchemaMapper.getSchema(mapType.getKeysType(), false);
        if (keySchema.getType() == Schema.Type.STRING) {
            // nothing needs to be done; return map.
            return deserializedMap;
        } else {
            // squish the keys down into strings.
            Map<String, Object> finalMap = new  HashMap<>(deserializedMap.size());
            for (Map.Entry<?, ?> entry : deserializedMap.entrySet()) {
                finalMap.put(entry.getKey().toString(), entry.getValue());
            }
            return finalMap;
        }

    }

    @Override
    public Schema getSchema(AbstractType<?> abstractType) {
        MapType<?, ?> mapType = (MapType<?, ?>) abstractType;
        // avro only allows the map key types to be strings, so we ignore the key type.
        AbstractType<?> valuesType = mapType.getValuesType();
        Schema valuesSchema = CassandraTypeToAvroSchemaMapper.getSchema(valuesType, false);
        return SchemaBuilder.map().values(valuesSchema);
    }
}

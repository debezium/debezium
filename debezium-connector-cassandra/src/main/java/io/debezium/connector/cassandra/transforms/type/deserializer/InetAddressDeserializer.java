/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import io.debezium.connector.cassandra.transforms.CassandraTypeToAvroSchemaMapper;
import org.apache.cassandra.db.marshal.AbstractType;

import java.net.InetAddress;
import java.nio.ByteBuffer;

public class InetAddressDeserializer extends BasicTypeDeserializer {

    public InetAddressDeserializer() {
        super(CassandraTypeToAvroSchemaMapper.STRING_TYPE);
    }

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        InetAddress inetAddress = (InetAddress) super.deserialize(abstractType, bb);
        return inetAddress.toString();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class UserTypeDeserializer extends TypeDeserializer {

    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        ByteBuffer userTypeByteBuffer = (ByteBuffer) super.deserialize(abstractType, bb);
        UserType userType = (UserType) abstractType;

        UserTypes.Value value = UserTypes.Value.fromSerialized(userTypeByteBuffer, userType);
        List<ByteBuffer> elements = value.getElements();

        Struct struct = new Struct(getSchemaBuilder(abstractType).build());

        for (int i = 0; i < userType.fieldNames().size(); i++) {
            String fieldName = userType.fieldNameAsString(i);
            AbstractType<?> fieldType = userType.type(i);
            struct.put(fieldName, CassandraTypeDeserializer.deserialize(fieldType, elements.get(i)));
        }

        return struct;
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        UserType userType = (UserType) abstractType;
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(userType.keyspace + "." + userType.getNameAsString());
        List<org.apache.cassandra.cql3.FieldIdentifier> fieldIdentifiers = userType.fieldNames();
        List<AbstractType<?>> fieldTypes = userType.fieldTypes();
        for (int i = 0; i < fieldIdentifiers.size(); i++) {
            Schema fieldSchema = CassandraTypeDeserializer.getSchemaBuilder(fieldTypes.get(i)).build();
            schemaBuilder.field(fieldIdentifiers.get(i).toString(), fieldSchema);
        }
        return schemaBuilder.optional();
    }
}

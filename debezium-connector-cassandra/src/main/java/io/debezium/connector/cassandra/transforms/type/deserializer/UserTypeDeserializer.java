/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.connector.cassandra.transforms.CassandraTypeToAvroSchemaMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;

import java.nio.ByteBuffer;
import java.util.List;

public class UserTypeDeserializer extends TypeDeserializer {

    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        ByteBuffer userTypeByteBuffer = (ByteBuffer) super.deserialize(abstractType, bb);
        UserType userType = (UserType) abstractType;

        UserTypes.Value value = UserTypes.Value.fromSerialized(userTypeByteBuffer, userType);
        List<ByteBuffer> elements = value.getElements();

        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(getSchema(abstractType));

        for (int i = 0; i < userType.fieldNames().size(); i++) {
            String fieldName = userType.fieldNameAsString(i);
            AbstractType<?> fieldType = userType.type(i);
            recordBuilder.set(fieldName, CassandraTypeDeserializer.deserialize(fieldType, elements.get(i)));
        }

        return recordBuilder.build();
    }

    @Override
    public Schema getSchema(AbstractType<?> abstractType) {
        UserType userType = (UserType) abstractType;
        SchemaBuilder.FieldAssembler<Schema> schemaBuilder =
                SchemaBuilder.record(userType.getNameAsString())
                             .namespace(userType.keyspace)
                             .fields();
        List<org.apache.cassandra.cql3.FieldIdentifier> fieldIdentifiers = userType.fieldNames();
        List<AbstractType<?>> fieldTypes = userType.fieldTypes();
        for (int i = 0; i < fieldIdentifiers.size(); i++) {
            Schema fieldSchema = CassandraTypeToAvroSchemaMapper.getSchema(fieldTypes.get(i), false);
            schemaBuilder.name(fieldIdentifiers.get(i).toString()).type(fieldSchema).noDefault();
        }
        return schemaBuilder.endRecord();
    }
}

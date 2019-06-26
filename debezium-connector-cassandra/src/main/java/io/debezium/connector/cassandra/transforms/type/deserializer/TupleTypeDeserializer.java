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
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;

import java.nio.ByteBuffer;
import java.util.List;

public class TupleTypeDeserializer extends TypeDeserializer {

    private static final String TUPLE_NAME_POSTFIX = "Tuple";
    private static final String FIELD_NAME_PREFIX = "field";

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        // the fun single case were we don't actually want to do the default deserialization!
        TupleType tupleType = (TupleType) abstractType;
        List<AbstractType<?>> innerTypes = tupleType.allTypes();
        ByteBuffer[] innerValueByteBuffers = tupleType.split(bb);

        GenericRecordBuilder builder = new GenericRecordBuilder(getSchema(abstractType));

        for (int i = 0; i < innerTypes.size(); i++) {
            AbstractType<?> currentInnerType = innerTypes.get(i);
            String fieldName = createFieldNameForIndex(i);
            Object deserializedInnerObject =
                    CassandraTypeDeserializer.deserialize(currentInnerType, innerValueByteBuffers[i]);
            builder.set(fieldName, deserializedInnerObject);
        }

        return builder.build();
    }

    @Override
    public Schema getSchema(AbstractType<?> abstractType) {
        TupleType tupleType = (TupleType) abstractType;
        List<AbstractType<?>> tupleInnerTypes = tupleType.allTypes();

        String recordName = createTupleName(tupleInnerTypes);

        SchemaBuilder.FieldAssembler<Schema> schemaBuilder = SchemaBuilder.record(recordName).fields();

        for (int i = 0; i < tupleInnerTypes.size(); i++) {
            AbstractType<?> innerType = tupleInnerTypes.get(i);
            schemaBuilder.name(createFieldNameForIndex(i))
                         .type(CassandraTypeToAvroSchemaMapper.getSchema(innerType, false))
                         .noDefault();
        }

        return schemaBuilder.endRecord();
    }

    private String createTupleName(List<AbstractType<?>> innerTypes) {
        StringBuilder tupleNameBuilder = new StringBuilder();
        for (AbstractType<?> innerType : innerTypes) {
            tupleNameBuilder.append(abstractTypeToNiceString(innerType));
        }
        return tupleNameBuilder.append(TUPLE_NAME_POSTFIX).toString();
    }

    private String createFieldNameForIndex(int i) {
        // begin indexing at 1
        return FIELD_NAME_PREFIX + (i + 1);
    }

    private String abstractTypeToNiceString(AbstractType<?> tupleInnerType) {
        // the full class name of the type. We want to pair it down to just the final type and remove the "Type".
        String typeName = tupleInnerType.getClass().getSimpleName();
        return typeName.substring(0, typeName.length() - 4);
    }
}

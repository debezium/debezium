/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class ListTypeDeserializer extends CollectionTypeDeserializer<ListType<?>> {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        List<?> deserializedList = (List<?>) super.deserialize(abstractType, bb);
        deserializedList = convertDeserializedElementsIfNecessary(abstractType, deserializedList);
        return Values.convertToList(getSchemaBuilder(abstractType).build(), deserializedList);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        ListType<?> listType = (ListType<?>) abstractType;
        AbstractType<?> elementsType = listType.getElementsType();
        Schema innerSchema = CassandraTypeDeserializer.getSchemaBuilder(elementsType).build();
        return SchemaBuilder.array(innerSchema).optional();
    }

    @Override
    public Object deserialize(ListType<?> listType, ComplexColumnData ccd) {
        List<ByteBuffer> bbList = listType.serializedValues(ccd.iterator());
        AbstractType<?> elementsType = listType.getElementsType();
        List<Object> deserializedList = new ArrayList<>(bbList.size());
        for (ByteBuffer bb : bbList) {
            deserializedList.add(CassandraTypeDeserializer.deserialize(elementsType, bb));
        }
        return Values.convertToList(getSchemaBuilder(listType).build(), deserializedList);
    }

    /**
     * If elements in a deserialized list is LogicalType, convert each element to fit in Kafka Schema type
     * @param abstractType the {@link AbstractType} of a column in Cassandra
     * @param deserializedList List deserialized from Cassandra
     * @return A deserialized list from Cassandra with each element that fits in Kafka Schema type
     */
    private List<?> convertDeserializedElementsIfNecessary(AbstractType<?> abstractType, List<?> deserializedList) {
        AbstractType<?> elementsType = ((ListType<?>) abstractType).getElementsType();
        TypeDeserializer elementsTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(elementsType);
        if (elementsTypeDeserializer instanceof LogicalTypeDeserializer) {
            List<Object> convertedDeserializedList = new ArrayList<>(deserializedList.size());
            for (Object element : deserializedList) {
                Object convertedElement = ((LogicalTypeDeserializer) elementsTypeDeserializer).convertDeserializedValue(elementsType, element);
                convertedDeserializedList.add(convertedElement);
            }
            return convertedDeserializedList;
        }
        return deserializedList;
    }
}

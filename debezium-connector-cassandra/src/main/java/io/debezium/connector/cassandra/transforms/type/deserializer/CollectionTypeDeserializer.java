package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.rows.ComplexColumnData;

public abstract class CollectionTypeDeserializer extends TypeDeserializer {
    public abstract Object deserialize(CollectionType<?> collectionType, ComplexColumnData ccd);
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.rows.ComplexColumnData;

public abstract class CollectionTypeDeserializer<T extends CollectionType<?>> extends TypeDeserializer {
    public abstract Object deserialize(T collectionType, ComplexColumnData ccd);
}

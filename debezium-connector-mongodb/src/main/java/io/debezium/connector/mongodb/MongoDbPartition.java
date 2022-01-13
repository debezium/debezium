/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Map;

import io.debezium.pipeline.spi.Partition;

public class MongoDbPartition implements Partition {

    @Override
    public Map<String, String> getSourcePartition() {
        throw new UnsupportedOperationException("Currently unsupported by the MongoDB connector");
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("Currently unsupported by the MongoDB connector");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Currently unsupported by the MongoDB connector");
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException("Currently unsupported by the MongoDB connector");
    }
}

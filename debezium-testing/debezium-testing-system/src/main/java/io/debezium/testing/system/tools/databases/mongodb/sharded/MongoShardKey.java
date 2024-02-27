/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class MongoShardKey {
    private final String collection;
    private final String key;
    private final ShardingType shardingType;
    private final List<ShardKeyRange> keyRanges;

    public MongoShardKey(String collection, String key, ShardingType shardingType) {
        this.collection = collection;
        this.key = key;
        this.shardingType = shardingType;
        keyRanges = new LinkedList<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoShardKey that = (MongoShardKey) o;
        return Objects.equals(collection, that.collection) && Objects.equals(key, that.key);
    }

    public String getCollection() {
        return collection;
    }

    public String getKey() {
        return key;
    }

    public ShardingType getShardingType() {
        return shardingType;
    }

    public List<ShardKeyRange> getKeyRanges() {
        return keyRanges;
    }

    @Override
    public int hashCode() {
        return Objects.hash(collection, key);
    }

    public enum ShardingType {
        HASHED("\"hashed\""),
        RANGED("1");

        private final String value;

        ShardingType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}

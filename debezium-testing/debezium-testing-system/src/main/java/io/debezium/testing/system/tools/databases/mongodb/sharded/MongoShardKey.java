/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import lombok.Getter;

@Getter
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

    @Override
    public int hashCode() {
        return Objects.hash(collection, key);
    }

    @Getter
    public enum ShardingType {
        HASHED("\"hashed\""),
        NORMAL("1");

        private final String value;

        ShardingType(String value) {
            this.value = value;
        }
    }
}

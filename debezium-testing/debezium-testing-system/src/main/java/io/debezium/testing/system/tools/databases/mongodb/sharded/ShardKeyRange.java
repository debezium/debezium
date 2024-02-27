/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

/**
 * Range used for ranged collection sharding
 */
public class ShardKeyRange {
    private final String shardName;
    private final String start;
    private final String end;

    public ShardKeyRange(String shardName, String start, String end) {
        this.shardName = shardName;
        this.start = start;
        this.end = end;
    }

    public String getShardName() {
        return shardName;
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }
}

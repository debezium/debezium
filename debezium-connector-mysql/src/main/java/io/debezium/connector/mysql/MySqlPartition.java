/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;

public class MySqlPartition implements Partition {

    @Override
    public Map<String, String> getSourcePartition() {
        return Collections.emptyMap();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "MySqlPartition";
    }

    public static class Provider implements Partition.Provider<MySqlPartition> {

        @Override
        public Set<MySqlPartition> getPartitions() {
            return Collections.singleton(new MySqlPartition());
        }
    }
}

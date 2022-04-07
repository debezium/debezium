/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;

public class OraclePartition implements Partition {

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
        return "OraclePartition";
    }

    static class Provider implements Partition.Provider<OraclePartition> {

        @Override
        public Set<OraclePartition> getPartitions() {
            return Collections.singleton(new OraclePartition());
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.cockroachdb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.debezium.connector.common.AbstractPartition;

/**
 * Represents the logical partition used by the CockroachDB connector.
 * In Kafka Connect, a "partition" is a unit of offset tracking, not necessarily a database partition.
 * Since CockroachDB changefeeds are table-scoped and the connector currently streams from a single source,
 * we define a singleton logical partition per connector instance.
 *
 * If future versions support parallelized per-table changefeeds, this class can be extended accordingly.
 *
 * @author Virag Tripathi
 */
public class CockroachDBPartition extends AbstractPartition {

    private static final String PARTITION_KEY = "cockroachdb";

    private final String logicalName;

    public CockroachDBPartition(String logicalName) {
        this.logicalName = logicalName;
    }

    public String getLogicalName() {
        return logicalName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        Map<String, String> map = new HashMap<>();
        map.put(PARTITION_KEY, logicalName);
        return Collections.unmodifiableMap(map);
    }

    @Override
    public String toString() {
        return "CockroachDBPartition{" +
                "logicalName='" + logicalName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof CockroachDBPartition))
            return false;
        CockroachDBPartition that = (CockroachDBPartition) o;
        return Objects.equals(logicalName, that.logicalName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalName);
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

/**
 * Describes the source partition details for a binlog-based connector.
 *
 * @author Chris Cranford
 */
public class BinlogPartition extends AbstractPartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";
    private final String serverName;

    public BinlogPartition(String serverName, String databaseName) {
        super(databaseName);
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BinlogPartition that = (BinlogPartition) o;
        return Objects.equals(serverName, that.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName);
    }

    @Override
    public String toString() {
        return "BinlogPartition{" +
                "serverName='" + serverName + '\'' +
                "} " + super.toString();
    }
}

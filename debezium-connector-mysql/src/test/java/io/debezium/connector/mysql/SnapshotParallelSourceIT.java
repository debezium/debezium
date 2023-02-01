/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;

import org.junit.Ignore;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.junit.SkipWhenDatabaseVersion;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public class SnapshotParallelSourceIT extends SnapshotSourceIT {

    @Override
    protected Configuration.Builder simpleConfig() {
        return super.simpleConfig().with(MySqlConnectorConfig.SNAPSHOT_MAX_THREADS, 3);
    }

    @Ignore
    @Test
    @Override
    public void shouldSnapshotTablesInRowCountOrderAsc() {

    }

    @Ignore
    @Test
    @Override
    public void shouldSnapshotTablesInRowCountOrderDesc() {

    }

    @Ignore
    @Test
    @Override
    public void shouldSnapshotTablesInLexicographicalOrder() {

    }

    @Ignore
    @Test
    @Override
    public void shouldSnapshotTablesInOrderSpecifiedInTableIncludeList() {

    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Ignore;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Collect;

import ch.qos.logback.classic.Level;

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

    @Ignore
    @Test
    @Override
    public void shouldSnapshotTablesInOrderSpecifiedInTableIncludeListWithConflictingNames() {

    }

    @Test
    public void shouldParallelCreateSnapshotSchema() throws Exception {
        List<String> includeDatabases = Collect.arrayListOf(DATABASE.getDatabaseName(), OTHER_DATABASE.getDatabaseName());
        config = simpleConfig()
                .with(MySqlConnectorConfig.SNAPSHOT_MODE, MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY)
                .with(MySqlConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.NONE)
                .with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, String.join(",", includeDatabases))
                .build();
        LogInterceptor logInterceptor = new LogInterceptor(MySqlSnapshotChangeEventSource.class);
        logInterceptor.setLoggerLevel(MySqlSnapshotChangeEventSource.class, Level.INFO);

        // Start the connector ...
        start(MySqlConnector.class, config);
        waitForSnapshotToBeCompleted("mysql", DATABASE.getServerName());

        SourceRecords sourceRecords = consumeRecordsByTopic(100);
        List<SourceRecord> schemaRecords = sourceRecords.recordsForTopic(DATABASE.getServerName());
        List<String> sourceDatabases = schemaRecords.stream()
                .map(record -> ((Struct) record.value()).get("databaseName").toString())
                .filter(includeDatabases::contains)
                .distinct()
                .collect(Collectors.toList());

        assertThat(sourceDatabases.size()).isEqualTo(2);
        assertThat(logInterceptor.containsMessage("Creating schema snapshot worker pool with 3 worker thread(s)")).isTrue();
    }
}

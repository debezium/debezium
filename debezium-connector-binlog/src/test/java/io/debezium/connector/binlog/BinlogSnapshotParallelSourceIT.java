/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import static io.debezium.junit.EqualityCheck.LESS_THAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.ConditionalFail;
import io.debezium.junit.Flaky;
import io.debezium.junit.SkipWhenDatabaseVersion;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Collect;

import ch.qos.logback.classic.Level;

@SkipWhenDatabaseVersion(check = LESS_THAN, major = 5, minor = 6, reason = "DDL uses fractional second data types, not supported until MySQL 5.6")
public abstract class BinlogSnapshotParallelSourceIT<C extends SourceConnector> extends BinlogSnapshotSourceIT<C> {

    @Rule
    public TestRule conditionalFail = new ConditionalFail();

    @Override
    protected Configuration.Builder simpleConfig() {
        return super.simpleConfig().with(BinlogConnectorConfig.SNAPSHOT_MAX_THREADS, 3);
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
    @Flaky("DBZ-7472")
    public void shouldCreateSnapshotOfSingleDatabaseWithoutGlobalLock() throws Exception {
        super.shouldCreateSnapshotOfSingleDatabaseWithoutGlobalLock();
    }

    @Test
    public void shouldParallelCreateSnapshotSchema() throws Exception {
        List<String> includeDatabases = Collect.arrayListOf(DATABASE.getDatabaseName(), OTHER_DATABASE.getDatabaseName());
        config = simpleConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .with(getSnapshotLockingModeField(), getSnapshotLockingModeNone())
                .with(BinlogConnectorConfig.DATABASE_INCLUDE_LIST, String.join(",", includeDatabases))
                .build();
        LogInterceptor logInterceptor = new LogInterceptor(BinlogSnapshotChangeEventSource.class);
        logInterceptor.setLoggerLevel(BinlogSnapshotChangeEventSource.class, Level.INFO);

        // Start the connector ...
        start(getConnectorClass(), config);
        waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

        AbstractConnectorTest.SourceRecords sourceRecords = consumeRecordsByTopic(100);
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

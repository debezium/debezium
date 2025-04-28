/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.AbstractSchemaHistoryTest;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;

import oracle.jdbc.OracleTypes;

/**
 * Unit tests for Oracle's database schema history.
 *
 * @author Chris Cranford
 */
public class OracleSchemaHistoryTest extends AbstractSchemaHistoryTest {

    @Override
    protected HistoryRecord getRenameCreateHistoryRecord() {
        return new HistoryRecord(
                Map.of("server", TestHelper.SERVER_NAME),
                Map.of("snapshot_scn", "1", "snapshot", true, "scn", "1", "snapshot_completed", false),
                TestHelper.getDatabaseName(),
                "DEBEZIUM",
                "CREATE TABLE \"DEBEZIUM\".\"DBZ4451A\" (\"ID\" NUMBER(9,0), PRIMARY KEY(\"ID\");",
                new TableChanges().create(getTableToBeRenamed()),
                Instant.now());
    }

    @Override
    protected HistoryRecord getRenameAlterHistoryRecord() {
        final Table oldTable = getTableToBeRenamed();
        final TableId oldTableId = oldTable.id();
        final Table table = oldTable.edit().tableId(getRenameTableId()).create();

        return new HistoryRecord(
                Map.of("server", TestHelper.SERVER_NAME),
                Map.of("snapshot_scn", "2", "scn", "2", "commit_scn", "2"),
                TestHelper.getDatabaseName(),
                "DEBEZIUM",
                "ALTER TABLE DBZ4451A RENAME TO DBZ4451B;",
                new TableChanges().rename(table, oldTableId),
                Instant.now());
    }

    @Override
    protected TableId getRenameTableId() {
        return TableId.parse("ORCLPDB1.DEBEZIUM.DBZ4451B");
    }

    @Override
    protected Offsets<Partition, OffsetContext> getOffsets() {
        final OraclePartition source = new OraclePartition(TestHelper.SERVER_NAME, TestHelper.getDatabaseName());
        final Configuration config = Configuration.empty()
                .edit()
                .with(CommonConnectorConfig.TOPIC_PREFIX, TestHelper.SERVER_NAME)
                .build();
        final OracleOffsetContext position = OracleOffsetContext.create()
                .logicalName(new OracleConnectorConfig(config))
                .scn(Scn.valueOf(999))
                .commitScn(CommitScn.valueOf(999L))
                .snapshotScn(Scn.valueOf(999))
                .snapshotPendingTransactions(Collections.emptyMap())
                .snapshotCompleted(true)
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();

        return Offsets.of(source, position);
    }

    @Override
    protected DdlParser getDdlParser() {
        return new OracleDdlParser();
    }

    private Table getTableToBeRenamed() {
        final TableId tableId = TableId.parse("ORCLPDB1.DEBEZIUM.DBZ4451A");
        return Table.editor()
                .tableId(tableId)
                .addColumn(Column.editor()
                        .name("ID")
                        .type("NUMBER")
                        .length(9)
                        .scale(0)
                        .jdbcType(OracleTypes.NUMERIC)
                        .optional(false)
                        .create())
                .setPrimaryKeyNames("ID")
                .create();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;

/**
 * A {@link StreamingChangeEventSource} for Oracle.
 *
 * @author Gunnar Morling
 */
public class OracleSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSnapshotChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;

    public OracleSnapshotChangeEventSource(OracleConnectorConfig connectorConfig, OracleConnection jdbcConnection,
                                           OracleDatabaseSchema schema, EventDispatcher<TableId> dispatcher, Clock clock,
                                           SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);

        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OracleOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            snapshotData = connectorConfig.getSnapshotMode().includeData();
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext<OraclePartition, OracleOffsetContext> prepare(OraclePartition partition)
            throws Exception {
        if (connectorConfig.getPdbName() != null) {
            jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
        }

        return new OracleSnapshotContext(partition, connectorConfig.getCatalogName());
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.getAllTableIds(ctx.catalogName);
        // this very slow approach(commented out), it took 30 minutes on an instance with 600 tables
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotLockingMode().usesLocking()) {
            ((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");

            try (Statement statement = jdbcConnection.connection().createStatement()) {
                for (TableId tableId : snapshotContext.capturedTables) {
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while locking table " + tableId);
                    }

                    LOGGER.debug("Locking table {}", tableId);
                    statement.execute("LOCK TABLE " + quote(tableId) + " IN ROW SHARE MODE");
                }
            }
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext)
            throws SQLException {
        if (connectorConfig.getSnapshotLockingMode().usesLocking()) {
            jdbcConnection.connection().rollback(((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx,
                                           OracleOffsetContext previousOffset)
            throws Exception {
        Optional<Scn> latestTableDdlScn = getLatestTableDdlScn(ctx);
        Scn currentScn;

        // we must use an SCN for taking the snapshot that represents a later timestamp than the latest DDL change than
        // any of the captured tables; this will not be a problem in practice, but during testing it may happen that the
        // SCN of "now" represents the same timestamp as a newly created table that should be captured; in that case
        // we'd get a ORA-01466 when running the flashback query for doing the snapshot
        do {
            currentScn = jdbcConnection.getCurrentScn();
        } while (areSameTimestamp(latestTableDdlScn.orElse(null), currentScn));

        ctx.offset = OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(currentScn)
                .transactionContext(new TransactionContext())
                .incrementalSnapshotContext(new SignalBasedIncrementalSnapshotContext<>())
                .build();
    }

    /**
     * Whether the two SCNs represent the same timestamp or not (resolution is only 3 seconds).
     */
    private boolean areSameTimestamp(Scn scn1, Scn scn2) throws SQLException {
        if (scn1 == null) {
            return false;
        }

        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT 1 FROM DUAL WHERE SCN_TO_TIMESTAMP(" + scn1 + ") = SCN_TO_TIMESTAMP(" + scn2 + ")")) {

            return rs.next();
        }
    }

    /**
     * Returns the SCN of the latest DDL change to the captured tables. The result will be empty if there's no table to
     * capture as per the configuration.
     */
    private Optional<Scn> getLatestTableDdlScn(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> ctx)
            throws SQLException {
        if (ctx.capturedTables.isEmpty()) {
            return Optional.empty();
        }

        StringBuilder lastDdlScnQuery = new StringBuilder("SELECT TIMESTAMP_TO_SCN(MAX(last_ddl_time))")
                .append(" FROM all_objects")
                .append(" WHERE");

        for (TableId table : ctx.capturedTables) {
            lastDdlScnQuery.append(" (owner = '" + table.schema() + "' AND object_name = '" + table.table() + "') OR");
        }

        String query = lastDdlScnQuery.substring(0, lastDdlScnQuery.length() - 3).toString();
        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery(query)) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get latest table DDL SCN");
            }

            // Guard against LAST_DDL_TIME with value of 0.
            // This case should be treated as if we were unable to determine a value for LAST_DDL_TIME.
            // This forces later calculations to be based upon the current SCN.
            String latestDdlTime = rs.getString(1);
            if ("0".equals(latestDdlTime)) {
                return Optional.empty();
            }

            return Optional.of(Scn.valueOf(latestDdlTime));
        }
        catch (SQLException e) {
            if (e.getErrorCode() == 8180) {
                // DBZ-1446 In this use case we actually do not want to propagate the exception but
                // rather return an empty optional value allowing the current SCN to take prior.
                LOGGER.info("No latest table SCN could be resolved, defaulting to current SCN");
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                      OracleOffsetContext offsetContext)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            // todo: DBZ-137 the new readSchemaForCapturedTables seems to cause failures.
            // For now, reverted to the default readSchema implementation as the intended goal
            // with the new implementation was to be faster, not change behavior.
            // if (connectorConfig.getAdapter().equals(OracleConnectorConfig.ConnectorAdapter.LOG_MINER)) {
            // jdbcConnection.readSchemaForCapturedTables(
            // snapshotContext.tables,
            // snapshotContext.catalogName,
            // schema,
            // connectorConfig.getColumnFilter(),
            // false,
            // snapshotContext.capturedTables);
            // }
            // else {
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
            // }
        }
    }

    @Override
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                             String overriddenSelect, TableId tableId) {
        String snapshotOffset = (String) snapshotContext.offset.getOffset().get(SourceInfo.SCN_KEY);
        String token = connectorConfig.getTokenToReplaceInSnapshotPredicate();
        if (token != null) {
            return overriddenSelect.replaceAll(token, " AS OF SCN " + snapshotOffset);
        }
        return overriddenSelect;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                                    Table table)
            throws SQLException {
        return new SchemaChangeEvent(
                snapshotContext.partition.getSourcePartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                jdbcConnection.getTableMetadataDdl(table.id()),
                table,
                SchemaChangeEventType.CREATE,
                true);
    }

    /**
     * Generate a valid Oracle query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        final OracleOffsetContext offset = snapshotContext.offset;
        final String snapshotOffset = offset.getScn().toString();
        String snapshotSelectColumns = columns.stream()
                .collect(Collectors.joining(", "));
        assert snapshotOffset != null;
        return Optional.of(String.format("SELECT %s FROM %s AS OF SCN %s", snapshotSelectColumns, quote(tableId), snapshotOffset));
    }

    @Override
    protected void complete(SnapshotContext<OraclePartition, OracleOffsetContext> snapshotContext) {
        if (connectorConfig.getPdbName() != null) {
            jdbcConnection.resetSessionToCdb();
        }
    }

    private static String quote(TableId tableId) {
        return TableId.parse(tableId.schema() + "." + tableId.table(), true).toDoubleQuotedString();
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class OracleSnapshotContext extends RelationalSnapshotContext<OraclePartition, OracleOffsetContext> {

        private Savepoint preSchemaSnapshotSavepoint;

        public OracleSnapshotContext(OraclePartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }
}

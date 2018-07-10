/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;

public class SqlServerOffsetContext implements OffsetContext {

    private static final String SERVER_PARTITION_KEY = "server";
    private static final String QUERY_FROM_LSN_KEY = "query_from_lsn";
    private static final String QUERY_TO_LSN_KEY = "query_to_lsn";
    private static final String QUERY_TABLE = "query_table";

    private final Schema sourceInfoSchema;
    private final SourceInfo sourceInfo;
    private final Map<String, String> partition;

    private Lsn queryFromLsn;
    private Lsn queryToLsn;
    private TableId queryTable;

    public SqlServerOffsetContext(String serverName) {
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, serverName);
        sourceInfo = new SourceInfo(serverName);
        sourceInfoSchema = sourceInfo.schema();
    }

    @Override
    public Map<String, ?> getPartition() {
        return partition;
    }

    @Override
    public Map<String, ?> getOffset() {
        return Collect.hashMapOf(
                QUERY_FROM_LSN_KEY, queryFromLsn == null ? null : queryFromLsn.toString(),
                QUERY_TO_LSN_KEY, queryToLsn == null ? null : queryToLsn.toString(),
                QUERY_TABLE, queryTable == null ? null : queryTable.toString(),
                SourceInfo.CHANGE_LSN_KEY, sourceInfo.getChangeLsn() == null ? null : sourceInfo.getChangeLsn().toString()
        );
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public void setChangeLsn(Lsn lsn) {
        sourceInfo.setChangeLsn(lsn);
    }

    public void setSourceTime(Instant instant) {
        sourceInfo.setSourceTime(instant);
    }

    public void setQueryFromLsn(Lsn queryFromLsn) {
        this.queryFromLsn = queryFromLsn;
    }

    public void setQueryToLsn(Lsn queryToLsn) {
        this.queryToLsn = queryToLsn;
    }

    public void setQueryTable(TableId queryTable) {
        this.queryTable = queryTable;
    }

    @Override
    public boolean isSnapshotRunning() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void preSnapshotStart() {
        // TODO Auto-generated method stub
    }

    @Override
    public void preSnapshotCompletion() {
        // TODO Auto-generated method stub
    }

    @Override
    public void postSnapshotCompletion() {
        // TODO Auto-generated method stub
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

/**
 * Information about the source of a change event captured from TiDB through TiCDC.
 * <p>
 * TiDB does not use binlog file/position or GTIDs for ordering. Transactions are ordered by the
 * commit timestamp ({@code commit_ts}) issued by the TSO (Timestamp Oracle) of the TiDB cluster.
 * The TiCDC Kafka stream position (topic/partition/offset) is tracked separately in the offsets;
 * the {@code source} block of emitted events exposes the logical TiDB position only.
 *
 * @author Aviral Srivastava
 */
@NotThreadSafe
public final class SourceInfo extends BaseSourceInfo {

    public static final String COMMIT_TS_KEY = "commit_ts";
    public static final String CLUSTER_ID_KEY = "cluster_id";

    private TableId tableId;
    private long commitTs;
    private String clusterId;
    private Instant sourceTime;

    public SourceInfo(TiDbConnectorConfig connectorConfig) {
        super(connectorConfig);
    }

    public void update(TableId tableId, Instant sourceTime, long commitTs, String clusterId) {
        this.tableId = tableId;
        this.sourceTime = sourceTime;
        this.commitTs = commitTs;
        this.clusterId = clusterId;
    }

    public TableId tableId() {
        return tableId;
    }

    public long commitTs() {
        return commitTs;
    }

    public String clusterId() {
        return clusterId;
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        return tableId != null ? tableId.catalog() : null;
    }

    String table() {
        return tableId != null ? tableId.table() : null;
    }

    @Override
    public String toString() {
        return "SourceInfo [tableId=" + tableId + ", commitTs=" + commitTs + ", clusterId=" + clusterId
                + ", sourceTime=" + sourceTime + "]";
    }
}

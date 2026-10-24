/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog.history;

import java.util.function.Consumer;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;

/**
 * A no-op {@link SchemaHistory} used by the binlog-metadata-based schema mode (debezium/dbz#978). In
 * this mode the streaming table schema is reconstructed from the FULL metadata carried by binlog
 * {@code TABLE_MAP} events, so no persistent schema history storage is needed: records are discarded and
 * recovery is a no-op, while the snapshot phase runs exactly as with a persistent history. The storage
 * always reports itself as present so that connector startup never attempts to create or recover it.
 */
@ThreadSafe
public final class BinlogMetadataSchemaHistory extends AbstractSchemaHistory {

    @Override
    protected void storeRecord(HistoryRecord record) {
        // The schema is rebuilt from the binlog TABLE_MAP metadata; nothing needs to be persisted.
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        // Nothing was persisted, so there is nothing to recover; the schema is rebuilt from the binlog.
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public String toString() {
        return "binlog-metadata-based (no persistent schema history)";
    }
}

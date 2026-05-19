/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.history;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.ydb.YdbInitialiser;
import io.debezium.storage.ydb.YdbTransport;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.values.PrimitiveValue;

/**
 * {@link SchemaHistory} backed by the {@code dbz_schema_history} table with PK
 * {@code (connector_name, db_name, seq_no)}. Keying by {@code db_name} (taken from the
 * record's {@code databaseName} field) lets a multi-task source connector (e.g. SQL Server
 * multi-partition mode) write history concurrently without coordination: each database is
 * always served by a single task, so the per-{@code (connector, db)} sequence has a single
 * writer. Recovery streams all rows for the connector ordered by {@code (db_name, seq_no)};
 * cross-database ordering is not required because DDL for a given schema always originates
 * from one task.
 *
 * <p>Records that carry no database (server-level DDL) are keyed under the empty string
 * fallback. Tables and parent directories are created on start by
 * {@link YdbInitialiser#createSchemaHistoryTable} (idempotent).
 */
@ThreadSafe
@Incubating
public final class YdbSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbSchemaHistory.class);

    private static final String UPSERT_RECORD_SQL = "DECLARE $connector AS Utf8;\n"
            + "DECLARE $db AS Utf8;\n"
            + "DECLARE $seq AS Uint64;\n"
            + "DECLARE $rec AS Utf8;\n"
            + "DECLARE $now AS Timestamp;\n"
            + "UPSERT INTO `%s` (connector_name, db_name, seq_no, record_json, created_at) "
            + "VALUES ($connector, $db, $seq, $rec, $now);";

    private static final String SELECT_ALL_SQL = "DECLARE $connector AS Utf8;\n"
            + "SELECT db_name, seq_no, record_json FROM `%s` "
            + "WHERE connector_name = $connector ORDER BY db_name, seq_no ASC;";

    private static final String SELECT_MAX_SEQ_PER_DB_SQL = "DECLARE $connector AS Utf8;\n"
            + "DECLARE $db AS Utf8;\n"
            + "SELECT MAX(seq_no) AS max_seq FROM `%s` "
            + "WHERE connector_name = $connector AND db_name = $db;";

    private static final String SELECT_EXISTS_SQL = "DECLARE $connector AS Utf8;\n"
            + "SELECT 1 FROM `%s` WHERE connector_name = $connector LIMIT 1;";

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();
    /** Next seq_no per db_name. Populated lazily on first write per db. */
    private final ConcurrentMap<String, Long> nextSeqByDb = new ConcurrentHashMap<>();

    private YdbSchemaHistoryConfig config;
    private GrpcTransport transport;
    private QueryClient queryClient;
    private SessionRetryContext retryCtx;

    private String upsertSql;
    private String selectAllSql;
    private String selectMaxSeqPerDbSql;
    private String selectExistsSql;

    @Override
    public void configure(Configuration config,
                          HistoryRecordComparator comparator,
                          SchemaHistoryListener listener,
                          boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.config = new YdbSchemaHistoryConfig(config);
    }

    @Override
    public synchronized void start() {
        super.start();
        if (!running.compareAndSet(false, true)) {
            return;
        }
        this.transport = YdbTransport.open(config);
        this.queryClient = QueryClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(queryClient).build();
        this.upsertSql = String.format(UPSERT_RECORD_SQL, config.getTableName());
        this.selectAllSql = String.format(SELECT_ALL_SQL, config.getTableName());
        this.selectMaxSeqPerDbSql = String.format(SELECT_MAX_SEQ_PER_DB_SQL, config.getTableName());
        this.selectExistsSql = String.format(SELECT_EXISTS_SQL, config.getTableName());

        YdbInitialiser.createSchemaHistoryTable(transport, queryClient, config.getDatabase(), config.getTableName());
        LOGGER.info("Started YdbSchemaHistory (table={}, connector={})",
                config.getTableName(), config.getConnectorName());
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (record == null) {
            return;
        }
        String dbName = extractDbName(record);
        try {
            String serialized = writer.write(record.document());
            long seq = nextSeqByDb.compute(dbName, (db, current) -> {
                long base = current != null ? current : loadMaxSeq(db) + 1;
                return base + 1;
            }) - 1;
            Params p = Params.of(
                    "$connector", PrimitiveValue.newText(config.getConnectorName()),
                    "$db", PrimitiveValue.newText(dbName),
                    "$seq", PrimitiveValue.newUint64(seq),
                    "$rec", PrimitiveValue.newText(serialized),
                    "$now", PrimitiveValue.newTimestamp(Instant.now()));
            retryCtx.supplyResult(session -> session.createQuery(upsertSql, TxMode.SERIALIZABLE_RW, p).execute())
                    .join().getStatus().expectSuccess("UPSERT " + config.getTableName());
        }
        catch (Exception e) {
            throw new SchemaHistoryException("Failed to store schema history record into " + config.getTableName(), e);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (queryClient != null) {
                queryClient.close();
                queryClient = null;
            }
            if (transport != null) {
                transport.close();
                transport = null;
            }
            nextSeqByDb.clear();
        }
        super.stop();
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        try {
            Params p = Params.of("$connector", PrimitiveValue.newText(config.getConnectorName()));
            QueryReader result = retryCtx.supplyResult(session -> QueryReader.readFrom(session.createQuery(selectAllSql, TxMode.SNAPSHOT_RO, p)))
                    .join().getValue();
            if (result.getResultSetCount() == 0) {
                return;
            }
            ResultSetReader rs = result.getResultSet(0);
            int recIdx = rs.getColumnIndex("record_json");
            while (rs.next()) {
                records.accept(new HistoryRecord(reader.read(rs.getColumn(recIdx).getText())));
            }
        }
        catch (Exception e) {
            throw new SchemaHistoryException("Failed to recover schema history from " + config.getTableName(), e);
        }
    }

    private long loadMaxSeq(String dbName) {
        Params p = Params.of(
                "$connector", PrimitiveValue.newText(config.getConnectorName()),
                "$db", PrimitiveValue.newText(dbName));
        QueryReader result = retryCtx.supplyResult(session -> QueryReader.readFrom(session.createQuery(selectMaxSeqPerDbSql, TxMode.SNAPSHOT_RO, p)))
                .join().getValue();
        if (result.getResultSetCount() == 0) {
            return -1L;
        }
        ResultSetReader rs = result.getResultSet(0);
        if (!rs.next()) {
            return -1L;
        }
        var col = rs.getColumn(rs.getColumnIndex("max_seq"));
        return col.isOptionalItemPresent() ? col.getOptionalItem().getUint64() : -1L;
    }

    private static String extractDbName(HistoryRecord record) {
        String db = record.document().getString(HistoryRecord.Fields.DATABASE_NAME);
        return db == null ? "" : db;
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public boolean exists() {
        if (!running.get()) {
            return false;
        }
        Params p = Params.of("$connector", PrimitiveValue.newText(config.getConnectorName()));
        QueryReader result = retryCtx.supplyResult(session -> QueryReader.readFrom(session.createQuery(selectExistsSql, TxMode.SNAPSHOT_RO, p)))
                .join().getValue();
        if (result.getResultSetCount() == 0) {
            return false;
        }
        return result.getResultSet(0).next();
    }

    @Override
    public String toString() {
        return "YDB schema history table=" + (config == null ? "?" : config.getTableName())
                + " connector=" + (config == null ? "?" : config.getConnectorName());
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.history;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
 * {@link SchemaHistory} backed by the {@code dbz_schema_history} table. The table and its
 * parent directories are created on start via {@link YdbInitialiser#createSchemaHistoryTable} (idempotent).
 * Records are appended with a monotonically growing {@code seq_no} (per connector); recovery
 * is a single {@code ORDER BY seq_no ASC} scan.
 *
 * <p>By design only one source instance writes per connector_name (one PG slot reader),
 * so the in-memory {@link AtomicLong} sequence is safe.
 */
@ThreadSafe
@Incubating
public final class YdbSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbSchemaHistory.class);

    private static final String UPSERT_RECORD_SQL = "DECLARE $connector AS Utf8;\n"
            + "DECLARE $seq AS Uint64;\n"
            + "DECLARE $rec AS Utf8;\n"
            + "DECLARE $now AS Timestamp;\n"
            + "UPSERT INTO `%s` (connector_name, seq_no, record_json, created_at) "
            + "VALUES ($connector, $seq, $rec, $now);";

    private static final String SELECT_ALL_SQL = "DECLARE $connector AS Utf8;\n"
            + "SELECT seq_no, record_json FROM `%s` "
            + "WHERE connector_name = $connector ORDER BY seq_no ASC;";

    private static final String SELECT_MAX_SEQ_SQL = "DECLARE $connector AS Utf8;\n"
            + "SELECT MAX(seq_no) AS max_seq FROM `%s` "
            + "WHERE connector_name = $connector;";

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong nextSeq = new AtomicLong();

    private YdbSchemaHistoryConfig config;
    private GrpcTransport transport;
    private QueryClient queryClient;
    private SessionRetryContext retryCtx;

    private String upsertSql;
    private String selectAllSql;
    private String selectMaxSeqSql;

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
        this.selectMaxSeqSql = String.format(SELECT_MAX_SEQ_SQL, config.getTableName());

        YdbInitialiser.createSchemaHistoryTable(transport, queryClient, config.getDatabase(), config.getTableName());
        nextSeq.set(loadMaxSeq() + 1);
        LOGGER.info("Started YdbSchemaHistory (table={}, connector={}, nextSeq={})",
                config.getTableName(), config.getConnectorName(), nextSeq.get());
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (record == null) {
            return;
        }
        try {
            String serialized = writer.write(record.document());
            long seq = nextSeq.getAndIncrement();
            Params p = Params.of(
                    "$connector", PrimitiveValue.newText(config.getConnectorName()),
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

    private long loadMaxSeq() {
        Params p = Params.of("$connector", PrimitiveValue.newText(config.getConnectorName()));
        QueryReader result = retryCtx.supplyResult(session -> QueryReader.readFrom(session.createQuery(selectMaxSeqSql, TxMode.SNAPSHOT_RO, p)))
                .join().getValue();
        if (result.getResultSetCount() == 0) {
            return -1L;
        }
        ResultSetReader rs = result.getResultSet(0);
        if (!rs.next()) {
            return -1L;
        }
        var col = rs.getColumn(rs.getColumnIndex("max_seq"));
        // MAX() over zero rows returns NULL; treat as "no records yet".
        try {
            return col.getUint64();
        }
        catch (Exception ignore) {
            return -1L;
        }
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
        return loadMaxSeq() >= 0;
    }

    @Override
    public String toString() {
        return "YDB schema history table=" + (config == null ? "?" : config.getTableName())
                + " connector=" + (config == null ? "?" : config.getConnectorName());
    }
}
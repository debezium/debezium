/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb.offset;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
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
 * {@link OffsetBackingStore} backed by the YDB {@code dbz_offsets} table. The table and its
 * parent directories are created on start via {@link YdbInitialiser#createOffsetTable} (idempotent).
 * Keyed by {@code (connector_name, partition_key)}; values are opaque UTF-8 strings (Connect's
 * serialized offset map).
 */
public class YdbOffsetBackingStore implements OffsetBackingStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbOffsetBackingStore.class);

    private static final String SELECT_ALL_SQL = "DECLARE $connector AS Utf8;\n"
            + "SELECT partition_key, offset_value FROM `%s` WHERE connector_name = $connector;";

    private static final String UPSERT_HEADER_SQL = "UPSERT INTO `%s` "
            + "(connector_name, partition_key, offset_value, updated_at) VALUES ";

    private YdbOffsetBackingStoreConfig config;
    private ExecutorService executor;
    private GrpcTransport transport;
    private QueryClient queryClient;
    private SessionRetryContext retryCtx;

    private String selectAllSql;
    private String upsertHeaderSql;

    protected ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();

    @Override
    public void configure(WorkerConfig workerConfig) {
        Configuration configuration = Configuration.from(workerConfig.originalsStrings());
        this.config = new YdbOffsetBackingStoreConfig(configuration);
    }

    @Override
    public synchronized void start() {
        executor = Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
                this.getClass().getSimpleName() + "-%d", false));
        this.transport = YdbTransport.open(config);
        this.queryClient = QueryClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(queryClient).build();
        this.selectAllSql = String.format(SELECT_ALL_SQL, config.getTableName());
        this.upsertHeaderSql = String.format(UPSERT_HEADER_SQL, config.getTableName());

        LOGGER.info("Starting YdbOffsetBackingStore (db={}, table={}, connector={})",
                config.getDatabase(), config.getTableName(), config.getConnectorName());
        YdbInitialiser.createOffsetTable(transport, queryClient, config.getDatabase(), config.getTableName());
        load();
    }

    @Override
    public synchronized void stop() {
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
        if (queryClient != null) {
            queryClient.close();
            queryClient = null;
        }
        if (transport != null) {
            transport.close();
            transport = null;
        }
        LOGGER.info("Stopped YdbOffsetBackingStore");
    }

    private void load() {
        Params p = Params.of("$connector", PrimitiveValue.newText(config.getConnectorName()));
        QueryReader result = retryCtx.supplyResult(session -> QueryReader.readFrom(session.createQuery(selectAllSql, TxMode.SNAPSHOT_RO, p)))
                .join().getValue();
        if (result.getResultSetCount() == 0) {
            return;
        }
        ResultSetReader rs = result.getResultSet(0);
        int kIdx = rs.getColumnIndex("partition_key");
        int vIdx = rs.getColumnIndex("offset_value");
        while (rs.next()) {
            data.put(rs.getColumn(kIdx).getText(), rs.getColumn(vIdx).getText());
        }
        LOGGER.info("Loaded {} offset entries", data.size());
    }

    protected void save() {
        if (data.isEmpty()) {
            return;
        }
        StringBuilder declares = new StringBuilder("DECLARE $connector AS Utf8;\nDECLARE $now AS Timestamp;\n");
        StringBuilder values = new StringBuilder(upsertHeaderSql);
        Params params = Params.create();
        params.put("$connector", PrimitiveValue.newText(config.getConnectorName()));
        params.put("$now", PrimitiveValue.newTimestamp(Instant.now()));

        int i = 0;
        for (Map.Entry<String, String> e : data.entrySet()) {
            String kVar = "$k" + i;
            String vVar = "$v" + i;
            declares.append("DECLARE ").append(kVar).append(" AS Utf8;\n")
                    .append("DECLARE ").append(vVar).append(" AS Utf8?;\n");
            params.put(kVar, PrimitiveValue.newText(e.getKey()));
            // Offset value may legitimately be empty; treat null specially.
            params.put(vVar, PrimitiveValue.newText(e.getValue() == null ? "" : e.getValue()).makeOptional());
            if (i > 0) {
                values.append(", ");
            }
            values.append("($connector, ").append(kVar).append(", ").append(vVar).append(", $now)");
            i++;
        }
        String sql = declares.append(values).append(";").toString();
        retryCtx.supplyResult(session -> session.createQuery(sql, TxMode.SERIALIZABLE_RW, params).execute())
                .join().getStatus().expectSuccess("UPSERT " + config.getTableName());
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values, final Callback<Void> callback) {
        return executor.submit(() -> {
            for (Map.Entry<ByteBuffer, ByteBuffer> e : values.entrySet()) {
                if (e.getKey() == null) {
                    continue;
                }
                data.put(fromByteBuffer(e.getKey()), fromByteBuffer(e.getValue()));
            }
            try {
                save();
                if (callback != null) {
                    callback.onCompletion(null, null);
                }
            }
            catch (Exception ex) {
                if (callback != null) {
                    callback.onCompletion(ex, null);
                }
                throw new ConnectException(ex);
            }
            return null;
        });
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(() -> {
            Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
            for (ByteBuffer key : keys) {
                result.put(key, toByteBuffer(data.get(fromByteBuffer(key))));
            }
            return result;
        });
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return null;
    }

    private static String fromByteBuffer(ByteBuffer buf) {
        return buf == null ? null : StandardCharsets.UTF_8.decode(buf.asReadOnlyBuffer()).toString();
    }

    private static ByteBuffer toByteBuffer(String s) {
        return s == null ? null : ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }
}
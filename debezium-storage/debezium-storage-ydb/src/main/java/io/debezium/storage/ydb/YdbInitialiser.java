/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.query.QueryClient;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.scheme.SchemeClient;

/**
 * Idempotent provisioning of YDB tables used by Debezium storage. Both the offset store and
 * the schema history call into this class on start, so no external init script is required.
 */
public final class YdbInitialiser {

    private static final Logger LOGGER = LoggerFactory.getLogger(YdbInitialiser.class);

    private static final String OFFSET_TABLE_DDL = "CREATE TABLE IF NOT EXISTS `%s` ("
            + "connector_name Utf8, "
            + "partition_key Utf8, "
            + "offset_value Utf8, "
            + "updated_at Timestamp, "
            + "PRIMARY KEY (connector_name, partition_key))";

    private static final String SCHEMA_HISTORY_TABLE_DDL = "CREATE TABLE IF NOT EXISTS `%s` ("
            + "connector_name Utf8, "
            + "seq_no Uint64, "
            + "record_json Utf8, "
            + "created_at Timestamp, "
            + "PRIMARY KEY (connector_name, seq_no))";

    private YdbInitialiser() {
    }

    public static void createOffsetTable(GrpcTransport transport,
                                         QueryClient queryClient,
                                         String database,
                                         String tableName) {
        ensureParentDirectories(transport, database, tableName);
        executeDdl(queryClient, String.format(OFFSET_TABLE_DDL, tableName), tableName);
    }

    public static void createSchemaHistoryTable(GrpcTransport transport,
                                                QueryClient queryClient,
                                                String database,
                                                String tableName) {
        ensureParentDirectories(transport, database, tableName);
        executeDdl(queryClient, String.format(SCHEMA_HISTORY_TABLE_DDL, tableName), tableName);
    }

    private static void executeDdl(QueryClient queryClient, String ddl, String tableName) {
        SessionRetryContext retryCtx = SessionRetryContext.create(queryClient).build();
        retryCtx.supplyResult(session -> session.createQuery(ddl, TxMode.NONE).execute())
                .join().getStatus().expectSuccess("CREATE TABLE IF NOT EXISTS " + tableName);
        LOGGER.info("Ensured YDB table {}", tableName);
    }

    private static void ensureParentDirectories(GrpcTransport transport, String database, String tableName) {
        int slash = tableName.lastIndexOf('/');
        if (slash <= 0) {
            return;
        }
        String parentRelative = tableName.substring(0, slash);
        String db = database.endsWith("/") ? database.substring(0, database.length() - 1) : database;
        try (SchemeClient scheme = SchemeClient.newClient(transport).build()) {
            StringBuilder current = new StringBuilder(db);
            for (String part : parentRelative.split("/")) {
                if (part.isEmpty()) {
                    continue;
                }
                current.append('/').append(part);
                Status status = scheme.makeDirectory(current.toString()).join();
                if (!status.isSuccess() && status.getCode() != StatusCode.ALREADY_EXISTS) {
                    status.expectSuccess("makeDirectory " + current);
                }
            }
        }
    }
}
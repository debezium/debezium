/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import java.io.IOException;
import java.net.SocketException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

/**
 * TimescaleDB metadata registry that performs out-of-band queries of TimescaleDB catalog to get
 * the mappings between chunks, hypertables and aggregates.
 *
 * @author Jiri Pechanec
 *
 */
public class QueryInformationSchemaMetadata extends AbstractTimescaleDbMetadata {

    private static final String CATALOG_SCHEMA = "_timescaledb_catalog";

    private static final String QUERY_HYPERTABLE_TO_AGGREGATE = String.format(
            "SELECT ht.schema_name, ht.table_name, agg.user_view_schema, agg.user_view_name FROM %s.continuous_agg agg"
                    + " LEFT JOIN %s.hypertable ht ON agg.mat_hypertable_id = ht.id",
            CATALOG_SCHEMA, CATALOG_SCHEMA);
    private static final String QUERY_TIMESCALEDB_VERSION = "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'";
    private static final String QUERY_CHUNK_TO_HYPERTABLE_LEGACY = String.format(
            "SELECT c.schema_name, c.table_name, ht.schema_name, ht.table_name FROM %s.chunk c "
                    + "LEFT JOIN %s.hypertable ht ON c.hypertable_id = ht.id",
            CATALOG_SCHEMA, CATALOG_SCHEMA);
    private static final String QUERY_CHUNK_TO_HYPERTABLE_2_29 = String.format(
            "SELECT n.nspname, ch.relname, ht.schema_name, ht.table_name FROM %s.chunk c "
                    + "JOIN pg_class ch ON ch.oid = c.relid "
                    + "JOIN pg_namespace n ON n.oid = ch.relnamespace "
                    + "LEFT JOIN %s.hypertable ht ON c.hypertable_id = ht.id",
            CATALOG_SCHEMA, CATALOG_SCHEMA);

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryInformationSchemaMetadata.class);

    private final PostgresConnection connection;
    private final Map<TableId, TableId> chunkToHypertable = new HashMap<>();
    private final Map<TableId, TableId> hypertableToAggregate = new HashMap<>();
    private final String chunkToHypertableQuery;

    public QueryInformationSchemaMetadata(Configuration config) {
        super(config);
        connection = new PostgresConnection(
                JdbcConfiguration.adapt(config.subset(ConfigurationNames.DATABASE_CONFIG_PREFIX, true)
                        .merge(config.subset(CommonConnectorConfig.DRIVER_CONFIG_PREFIX, true))),
                "Debezium TimescaleDB metadata");
        chunkToHypertableQuery = resolveChunkToHypertableQuery();
    }

    private String resolveChunkToHypertableQuery() {
        try {
            final String timescaleDbVersion = connection.queryAndMap(QUERY_TIMESCALEDB_VERSION,
                    rs -> rs.next() ? rs.getString(1) : null);
            if (timescaleDbVersion == null) {
                throw new DebeziumException("TimescaleDB extension is not installed");
            }
            LOGGER.debug("Detected TimescaleDB version '{}'", timescaleDbVersion);
            return isTimescaleDbVersionAtLeast229(timescaleDbVersion)
                    ? QUERY_CHUNK_TO_HYPERTABLE_2_29
                    : QUERY_CHUNK_TO_HYPERTABLE_LEGACY;
        }
        catch (SQLException e) {
            if (isRetriable(e)) {
                retryTransientException(e, "Failed to determine TimescaleDB version");
            }
            throw new DebeziumException("Failed to determine TimescaleDB version", e);
        }
    }

    @Override
    public Optional<TableId> hypertableId(TableId chunkId) {
        final var hypertableId = chunkToHypertable.get(chunkId);
        if (hypertableId != null) {
            return Optional.of(hypertableId);
        }
        LOGGER.debug("Chunk '{}' not found, querying the catalog", chunkId);
        loadTimescaleMetadata();
        return Optional.ofNullable(chunkToHypertable.get(chunkId));
    }

    @Override
    public Optional<TableId> aggregateId(TableId hypertableId) {
        return Optional.ofNullable(hypertableToAggregate.get(hypertableId));
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    private void loadTimescaleMetadata() {
        try {
            chunkToHypertable.clear();
            connection.query(chunkToHypertableQuery, rs -> {
                while (rs.next()) {
                    chunkToHypertable.put(new TableId(null, rs.getString(1), rs.getString(2)),
                            new TableId(null, rs.getString(3), rs.getString(4)));
                }
            });

            hypertableToAggregate.clear();
            connection.query(QUERY_HYPERTABLE_TO_AGGREGATE, rs -> {
                while (rs.next()) {
                    hypertableToAggregate.put(new TableId(null, rs.getString(1), rs.getString(2)),
                            new TableId(null, rs.getString(3), rs.getString(4)));
                }
            });
        }
        catch (SQLException e) {
            if (isRetriable(e)) {
                retryTransientException(e, "Failed to read TimescaleDB metadata");
            }
            throw new DebeziumException("Failed to read TimescaleDB metadata", e);
        }
    }

    static boolean isRetriable(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLTransientException
                    || current instanceof SQLRecoverableException) {
                return true;
            }
            if (current instanceof SocketException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void retryTransientException(Exception e, String message) {
        try {
            connection.close();
        }
        catch (Exception closeError) {
            LOGGER.debug("Failed to close broken connection before reconnect", closeError);
        }
        try {
            connection.reconnect();
        }
        catch (SQLException reconnectError) {
            LOGGER.debug("Failed to reconnect after a retriable TimescaleDB metadata error", reconnectError);
        }
        throw new RetriableException(message, e);
    }

    static boolean isTimescaleDbVersionAtLeast229(String version) {
        final String[] parts = version.split("\\.");
        if (parts.length < 2) {
            throw new DebeziumException("Unable to parse TimescaleDB version '" + version + "'");
        }
        try {
            final int major = Integer.parseInt(parts[0]);
            final int minor = Integer.parseInt(parts[1]);
            return major > 2 || (major == 2 && minor >= 29);
        }
        catch (NumberFormatException e) {
            throw new DebeziumException("Unable to parse TimescaleDB version '" + version + "'", e);
        }
    }
}

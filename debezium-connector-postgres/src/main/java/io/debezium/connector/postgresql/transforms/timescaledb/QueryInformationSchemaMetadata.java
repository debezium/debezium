/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Pattern;

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
    private static final String QUERY_CHUNK_TO_HYPERTABLE = String.format(
            "SELECT c.schema_name, c.table_name, ht.schema_name, ht.table_name FROM %s.chunk c "
                    + "LEFT JOIN %s.hypertable ht ON c.hypertable_id = ht.id",
            CATALOG_SCHEMA, CATALOG_SCHEMA);
    private static final String QUERY_HYPERTABLES = String.format(
            "SELECT id, schema_name, table_name FROM %s.hypertable",
            CATALOG_SCHEMA);

    private static final Pattern DEFAULT_CHUNK_NAME = Pattern.compile("_hyper_(\\d{1,9})_\\d+_chunk");

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryInformationSchemaMetadata.class);

    private final PostgresConnection connection;
    private final Map<TableId, TableId> chunkToHypertable = new HashMap<>();
    private final Map<TableId, TableId> hypertableToAggregate = new HashMap<>();
    private final Map<Integer, TableId> hypertablesById = new HashMap<>();

    public QueryInformationSchemaMetadata(Configuration config) {
        super(config);
        connection = new PostgresConnection(
                JdbcConfiguration.adapt(config.subset(ConfigurationNames.DATABASE_CONFIG_PREFIX, true)
                        .merge(config.subset(CommonConnectorConfig.DRIVER_CONFIG_PREFIX, true))),
                "Debezium TimescaleDB metadata");
    }

    @Override
    public Optional<TableId> hypertableId(TableId chunkId) {
        final var hypertableId = chunkToHypertable.get(chunkId);
        if (hypertableId != null) {
            return Optional.of(hypertableId);
        }
        LOGGER.debug("Chunk '{}' not found, querying the catalog", chunkId);
        loadTimescaleMetadata();
        final var resolvedId = Optional.ofNullable(chunkToHypertable.get(chunkId))
                .or(() -> hypertableIdFromDroppedChunk(chunkId));
        resolvedId.ifPresent(id -> chunkToHypertable.put(chunkId, id));
        return resolvedId;
    }

    // A dropped chunk is gone from the catalog while its changes can still be in the WAL
    private Optional<TableId> hypertableIdFromDroppedChunk(TableId chunkId) {
        final var id = hypertableIdFromChunkName(chunkId.table());
        return id.isPresent() ? Optional.ofNullable(hypertablesById.get(id.getAsInt())) : Optional.empty();
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
            connection.query(QUERY_CHUNK_TO_HYPERTABLE, rs -> {
                while (rs.next()) {
                    chunkToHypertable.put(new TableId(null, rs.getString(1), rs.getString(2)),
                            new TableId(null, rs.getString(3), rs.getString(4)));
                }
            });

            hypertablesById.clear();
            connection.query(QUERY_HYPERTABLES, rs -> {
                while (rs.next()) {
                    hypertablesById.put(rs.getInt(1), new TableId(null, rs.getString(2), rs.getString(3)));
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
                throw new RetriableException("Failed to read TimescaleDB metadata", e);
            }
            throw new DebeziumException("Failed to read TimescaleDB metadata", e);
        }
    }

    static OptionalInt hypertableIdFromChunkName(String chunkName) {
        final var matcher = DEFAULT_CHUNK_NAME.matcher(chunkName);
        return matcher.matches() ? OptionalInt.of(Integer.parseInt(matcher.group(1))) : OptionalInt.empty();
    }

    static boolean isRetriable(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLTransientException
                    || current instanceof SQLRecoverableException) {
                return true;
            }
            if (current instanceof SocketException || current instanceof EOFException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}

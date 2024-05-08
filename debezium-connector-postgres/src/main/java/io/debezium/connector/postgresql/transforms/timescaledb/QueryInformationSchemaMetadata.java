/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryInformationSchemaMetadata.class);

    private final PostgresConnection connection;
    private final Map<TableId, TableId> chunkToHypertable = new HashMap<>();
    private final Map<TableId, TableId> hypertableToAggregate = new HashMap<>();

    public QueryInformationSchemaMetadata(Configuration config) {
        super(config);
        connection = new PostgresConnection(
                JdbcConfiguration.adapt(config.subset(CommonConnectorConfig.DATABASE_CONFIG_PREFIX, true)
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
            connection.query(QUERY_CHUNK_TO_HYPERTABLE, rs -> {
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

            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to read TimescaleDB metadata", e);
        }
    }
}

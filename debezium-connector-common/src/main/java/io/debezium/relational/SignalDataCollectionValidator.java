/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.util.Strings;

/**
 * Validates {@code signal.data.collection} at connector {@code validate()} time: table existence, accepted FQN
 * shape, and effective column count. Gated by {@code signal.data.collection.validation.enabled} (default
 * {@code false}). Existence and shape problems are always config errors; a wrong effective column count is an
 * error under 3 columns, or a warning over 3, since customers may store extra metadata columns on the signal
 * table alongside {@code id}/{@code type}/{@code data}, filtered down to the required three.
 *
 * @author Debezium Authors
 */
public class SignalDataCollectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalDataCollectionValidator.class);
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";
    private static final String INITIAL_ONLY_SNAPSHOT_MODE = "initial_only";
    private static final int REQUIRED_COLUMN_COUNT = 3;

    private SignalDataCollectionValidator() {
    }

    /**
     * Validates every configured signal data collection (multiple may be configured for multi-task deployments).
     * No-op unless enabled, streaming-capable, and the source channel is on.
     *
     * @param connection the database connection to probe with
     * @param connectorConfig the connector configuration
     * @param signalDataCollectionValue the config value to report errors against
     */
    public static void validate(JdbcConnection connection, RelationalDatabaseConnectorConfig connectorConfig, ConfigValue signalDataCollectionValue) {
        if (!connectorConfig.isSignalDataCollectionValidationEnabled()) {
            return;
        }
        if (INITIAL_ONLY_SNAPSHOT_MODE.equals(connectorConfig.getSnapshotMode().getValue())) {
            return;
        }
        if (!connectorConfig.getEnabledChannels().contains(SourceSignalChannel.CHANNEL_NAME)) {
            return;
        }

        for (String rawValue : connectorConfig.getSignalingDataCollectionIds()) {
            if (Strings.isNullOrBlank(rawValue)) {
                continue;
            }
            try {
                checkSignalDataCollection(connection, connectorConfig, rawValue, signalDataCollectionValue);
            }
            catch (SQLException | RuntimeException e) {
                LOGGER.warn("{} Could not validate signal data collection '{}'", LOG_PREFIX, rawValue, e);
            }
        }
    }

    private static void checkSignalDataCollection(JdbcConnection connection, RelationalDatabaseConnectorConfig connectorConfig, String rawValue,
                                                    ConfigValue signalDataCollectionValue)
            throws SQLException {
        TableId parsed = TableId.parse(rawValue, false);
        Set<TableId> matches = connection.readTableNames(parsed.catalog(), parsed.schema(), parsed.table(), null);
        if (matches.isEmpty()) {
            fail(signalDataCollectionValue, String.format("Signal data collection '%s' does not exist.", rawValue));
            return;
        }

        TableId resolved = matches.stream()
                .filter(connectorConfig::isSignalDataCollection)
                .findFirst()
                .orElse(null);
        if (resolved == null) {
            if (matches.size() == 1) {
                fail(signalDataCollectionValue, String.format("signal.data.collection must be '%s', not '%s'.", matches.iterator().next(), rawValue));
            }
            else {
                List<String> candidates = matches.stream().map(TableId::toString).sorted().toList();
                fail(signalDataCollectionValue, String.format("signal.data.collection must be one of %s, not '%s'.", candidates, rawValue));
            }
            return;
        }

        long effectiveColumnCount = countEffectiveColumns(connection, connectorConfig.getColumnFilter(), resolved);
        if (effectiveColumnCount == REQUIRED_COLUMN_COUNT) {
            LOGGER.info("{} Signal data collection '{}' is valid.", LOG_PREFIX, rawValue);
            return;
        }

        String message = String.format("Signal data collection '%s' has %d columns; exactly %d are required. Adjust the table "
                + "or column.include.list/column.exclude.list accordingly.",
                rawValue, effectiveColumnCount, REQUIRED_COLUMN_COUNT);
        if (effectiveColumnCount < REQUIRED_COLUMN_COUNT) {
            fail(signalDataCollectionValue, message);
        }
        else {
            LOGGER.warn("{} {}", LOG_PREFIX, message);
        }
    }

    /**
     * Counts the table's columns that survive the connector's real {@link ColumnNameFilter}, so the result matches
     * what actually reaches Debezium's schema - e.g. a {@code column.include.list} scoped to other tables can
     * legitimately reduce this to 0, which must be flagged.
     */
    private static long countEffectiveColumns(JdbcConnection connection, ColumnNameFilter columnFilter, TableId table) throws SQLException {
        return connection.getColumnNames(table).stream()
                .filter(column -> columnFilter.matches(table.catalog(), table.schema(), table.table(), column))
                .count();
    }

    private static void fail(ConfigValue signalDataCollectionValue, String problem) {
        LOGGER.warn("{} {}", LOG_PREFIX, problem);
        signalDataCollectionValue.addErrorMessage(problem);
    }
}

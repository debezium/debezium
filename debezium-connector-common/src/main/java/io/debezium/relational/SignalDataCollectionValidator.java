/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.util.Strings;

/**
 * Validates {@code signal.data.collection}: table existence, accepted FQN shape, and effective column count.
 * Existence and shape problems are always errors; a wrong effective column count is an error under 3 columns, or a
 * warning over 3, since customers may store extra metadata columns on the signal table alongside
 * {@code id}/{@code type}/{@code data}, filtered down to the required three.
 *
 * @author Debezium Authors
 */
public class SignalDataCollectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalDataCollectionValidator.class);
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";
    private static final int REQUIRED_COLUMN_COUNT = 3;

    private SignalDataCollectionValidator() {
    }

    /**
     * Validates every configured signal data collection (multiple may be configured for multi-task deployments).
     * No-op unless enabled, streaming-capable, and the source channel is on.
     */
    public static SignalDataCollectionValidationResult validate(SignalDataCollectionValidationRequest request) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        if (request.validationEnabled() && request.streamingCapable() && request.sourceChannelEnabled()) {
            for (String rawValue : request.rawValues()) {
                if (Strings.isNullOrBlank(rawValue)) {
                    continue;
                }
                try {
                    checkSignalDataCollection(request, rawValue, errors, warnings);
                }
                catch (SQLException | RuntimeException e) {
                    LOGGER.warn("{} Could not validate signal data collection '{}'", LOG_PREFIX, rawValue, e);
                }
            }
        }
        return new SignalDataCollectionValidationResult(errors, warnings);
    }

    private static void checkSignalDataCollection(SignalDataCollectionValidationRequest request, String rawValue, List<String> errors, List<String> warnings)
            throws SQLException {
        JdbcConnection connection = request.connectionResolver().apply(rawValue);
        TableId parsed = TableId.parse(rawValue, false);
        Set<TableId> matches = connection.readTableNames(parsed.catalog(), parsed.schema(), parsed.table(), null);
        if (matches.isEmpty()) {
            fail(errors, String.format("Signal data collection '%s' does not exist.", rawValue));
            return;
        }

        TableId resolved = matches.stream()
                .filter(request.isSignalDataCollection())
                .findFirst()
                .orElse(null);
        if (resolved == null) {
            if (matches.size() == 1) {
                fail(errors, String.format("signal.data.collection must be '%s', not '%s'.", matches.iterator().next(), rawValue));
            }
            else {
                List<String> candidates = matches.stream().map(TableId::toString).sorted().toList();
                fail(errors, String.format("signal.data.collection must be one of %s, not '%s'.", candidates, rawValue));
            }
            return;
        }

        long effectiveColumnCount = countEffectiveColumns(connection, request.columnFilter(), resolved);
        if (effectiveColumnCount == REQUIRED_COLUMN_COUNT) {
            LOGGER.info("{} Signal data collection '{}' is valid.", LOG_PREFIX, rawValue);
            return;
        }

        String message = String.format("Signal data collection '%s' has %d columns; exactly %d are required. Adjust the table "
                + "or column.include.list/column.exclude.list accordingly.",
                rawValue, effectiveColumnCount, REQUIRED_COLUMN_COUNT);
        if (effectiveColumnCount < REQUIRED_COLUMN_COUNT) {
            fail(errors, message);
        }
        else {
            LOGGER.warn("{} {}", LOG_PREFIX, message);
            warnings.add(message);
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

    private static void fail(List<String> errors, String problem) {
        LOGGER.warn("{} {}", LOG_PREFIX, problem);
        errors.add(problem);
    }
}

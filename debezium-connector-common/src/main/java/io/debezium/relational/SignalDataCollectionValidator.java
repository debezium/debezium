/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.util.Strings;

/**
 * Validates {@code signal.data.collection}: existence, accepted FQN shape, effective column count, column naming,
 * and sizing. Existence, shape, and an effective count other than 3 are errors; a first column not named
 * {@code id} is an error under insert_delete watermarking and otherwise a warning, as are the remaining names and
 * any undersized column, since those only risk truncating signal payloads.
 *
 * @author Debezium Authors
 */
public class SignalDataCollectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalDataCollectionValidator.class);
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";
    private static final int REQUIRED_COLUMN_COUNT = 3;
    private static final List<String> EXPECTED_COLUMN_NAMES = List.of("id", "type", "data");
    private static final List<Integer> MIN_COLUMN_SIZES = List.of(42, 32, 2000);

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
            errors.add(String.format("Signal data collection '%s' does not exist.", rawValue));
            return;
        }

        TableId resolved = matches.stream()
                .filter(request.isSignalDataCollection())
                .findFirst()
                .orElse(null);
        if (resolved == null) {
            if (matches.size() == 1) {
                errors.add(String.format("signal.data.collection must be '%s', not '%s'.", matches.iterator().next(), rawValue));
            }
            else {
                List<String> candidates = matches.stream().map(TableId::toString).sorted().toList();
                errors.add(String.format("signal.data.collection must be one of %s, not '%s'.", candidates, rawValue));
            }
            return;
        }

        List<Map.Entry<String, Integer>> effectiveColumns = effectiveColumns(connection, request.columnFilter(), resolved);
        if (effectiveColumns.size() != REQUIRED_COLUMN_COUNT) {
            errors.add(String.format("Signal data collection '%s' has %d columns; exactly %d are required. Adjust the table "
                    + "or column.include.list/column.exclude.list accordingly.",
                    rawValue, effectiveColumns.size(), REQUIRED_COLUMN_COUNT));
            return;
        }

        checkColumnNames(request.insertDeleteWatermarking(), rawValue, effectiveColumns, errors, warnings);
        checkColumnSizes(rawValue, effectiveColumns, warnings);
    }

    /** Returns the columns surviving the connector's {@link ColumnNameFilter}, so the count matches what reaches Debezium's schema. */
    private static List<Map.Entry<String, Integer>> effectiveColumns(JdbcConnection connection, ColumnNameFilter columnFilter, TableId table)
            throws SQLException {
        return connection.getColumnSizes(table).entrySet().stream()
                .filter(column -> columnFilter.matches(table.catalog(), table.schema(), table.table(), column.getKey()))
                .toList();
    }

    /** Flags columns not named id/type/data positionally; a misnamed first column is an error under insert_delete watermarking, otherwise a warning. */
    private static void checkColumnNames(boolean insertDeleteWatermarking, String rawValue, List<Map.Entry<String, Integer>> columns,
                                         List<String> errors, List<String> warnings) {
        for (int i = 0; i < REQUIRED_COLUMN_COUNT; i++) {
            String actualName = columns.get(i).getKey();
            String expectedName = EXPECTED_COLUMN_NAMES.get(i);
            if (actualName.equalsIgnoreCase(expectedName)) {
                continue;
            }
            if (i == 0 && insertDeleteWatermarking) {
                errors.add(String.format("Signal data collection '%s' first column must be named '%s' for the insert_delete watermarking "
                        + "strategy, but is '%s'.", rawValue, expectedName, actualName));
            }
            else {
                String message = String.format("Signal data collection '%s' column %d is named '%s'; '%s' is recommended.",
                        rawValue, i + 1, actualName, expectedName);
                LOGGER.warn("{} {}", LOG_PREFIX, message);
                warnings.add(message);
            }
        }
    }

    /** Warns when a positional column (id/type/data) is smaller than recommended; unknown sizes (e.g. LOBs) are skipped. */
    private static void checkColumnSizes(String rawValue, List<Map.Entry<String, Integer>> columns, List<String> warnings) {
        for (int i = 0; i < REQUIRED_COLUMN_COUNT; i++) {
            int size = columns.get(i).getValue();
            int minSize = MIN_COLUMN_SIZES.get(i);
            if (size > 0 && size < minSize) {
                String message = String.format("Signal data collection '%s' column '%s' has size %d; at least %d is recommended to avoid "
                        + "truncating signal payloads.", rawValue, columns.get(i).getKey(), size, minSize);
                LOGGER.warn("{} {}", LOG_PREFIX, message);
                warnings.add(message);
            }
        }
    }
}

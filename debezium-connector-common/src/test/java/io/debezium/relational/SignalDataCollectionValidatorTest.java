/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.Tables.ColumnNameFilter;

/**
 * Unit tests for {@link SignalDataCollectionValidator}: the enablement/gating checks, the ordered checks
 * (existence, accepted FQN shape, effective column count, column naming, column sizing), and the exception-swallowing guarantee.
 */
@ExtendWith(MockitoExtension.class)
public class SignalDataCollectionValidatorTest {

    private static final String RAW_VALUE = "dbo.debezium_signal";
    private static final ColumnNameFilter MATCH_ALL = (catalog, schema, table, column) -> true;
    private static final ColumnNameFilter MATCH_NONE = (catalog, schema, table, column) -> false;
    private static final Predicate<TableId> ALWAYS_SIGNAL_DATA_COLLECTION = table -> true;

    @Mock
    private JdbcConnection connection;

    private LogInterceptor logInterceptor;

    @BeforeEach
    public void beforeEach() {
        logInterceptor = new LogInterceptor(SignalDataCollectionValidator.class);
    }

    private SignalDataCollectionValidationRequest request(boolean validationEnabled, boolean streamingCapable, boolean sourceChannelEnabled,
                                                          boolean insertDeleteWatermarking, Predicate<TableId> isSignalDataCollection,
                                                          ColumnNameFilter columnFilter, String... rawValues) {
        return new SignalDataCollectionValidationRequest(Arrays.asList(rawValues), validationEnabled, streamingCapable, sourceChannelEnabled,
                insertDeleteWatermarking, rawValue -> connection, isSignalDataCollection, columnFilter);
    }

    private SignalDataCollectionValidationRequest enabledRequest(Predicate<TableId> isSignalDataCollection, ColumnNameFilter columnFilter, String... rawValues) {
        return request(true, true, true, false, isSignalDataCollection, columnFilter, rawValues);
    }

    private static Map<String, Integer> columnSizes(Object... nameThenSize) {
        Map<String, Integer> columns = new LinkedHashMap<>();
        for (int i = 0; i < nameThenSize.length; i += 2) {
            columns.put((String) nameThenSize[i], (Integer) nameThenSize[i + 1]);
        }
        return columns;
    }

    private static Map<String, Integer> wellSizedColumns() {
        return columnSizes("id", 42, "type", 32, "data", 2048);
    }

    @Test
    public void shouldDoNothingWhenValidationDisabled() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                request(false, true, true, false, ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        verifyNoInteractions(connection);
        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldDoNothingWhenNotStreamingCapable() {
        // initial_only never streams, so the signal channel never reads signal.data.collection; validating it would only mislead.
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                request(true, false, true, false, ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        verifyNoInteractions(connection);
        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldDoNothingWhenSourceChannelDisabled() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                request(true, true, false, false, ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        verifyNoInteractions(connection);
        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldSkipBlankSignalDataCollectionValues() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, (String) null, " "));

        verifyNoInteractions(connection);
        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldNotFailWhenSignalDataCollectionIsValid() throws SQLException {
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnSizes(resolved)).thenReturn(wellSizedColumns());

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.isValid()).isTrue();
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    public void shouldResolveCorrectCandidateAmongMultipleMatches() throws SQLException {
        // A 2-part FQN can match same-named tables in several catalogs; only the configured one is picked and inspected, regardless of Set order.
        TableId otherCatalogMatch = new TableId("otherDb", "dbo", "debezium_signal");
        TableId configuredMatch = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(otherCatalogMatch, configuredMatch));
        when(connection.getColumnSizes(configuredMatch)).thenReturn(wellSizedColumns());

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(configuredMatch::equals, MATCH_ALL, RAW_VALUE));

        assertThat(result.isValid()).isTrue();
        verify(connection, never()).getColumnSizes(otherCatalogMatch);
    }

    @Test
    public void shouldFailWhenTableDoesNotExist() throws SQLException {
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors()).containsExactly("Signal data collection '" + RAW_VALUE + "' does not exist.");
        verify(connection, never()).getColumnSizes(any());
    }

    @Test
    public void shouldFailWhenWrongShapeMatchesOneCandidate() throws SQLException {
        TableId found = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(found));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(table -> false, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors()).containsExactly("signal.data.collection must be '" + found + "', not '" + RAW_VALUE + "'.");
        verify(connection, never()).getColumnSizes(any());
    }

    @Test
    public void shouldFailWhenWrongShapeMatchesMultipleCandidates() throws SQLException {
        // The message must list every candidate, sorted for determinism, instead of picking one via Set iteration order.
        TableId dbTwoMatch = new TableId("db2", "dbo", "debezium_signal");
        TableId dbOneMatch = new TableId("db1", "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(dbTwoMatch, dbOneMatch));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(table -> false, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors())
                .containsExactly("signal.data.collection must be one of [" + dbOneMatch + ", " + dbTwoMatch + "], not '" + RAW_VALUE + "'.");
        verify(connection, never()).getColumnSizes(any());
    }

    @Test
    public void shouldFailWhenColumnFilterReducesSignalTableBelowRequiredColumns() throws SQLException {
        // Most common real-world trigger: a column.include.list scoped to other tables matches none of the signal table's columns.
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnSizes(resolved)).thenReturn(wellSizedColumns());

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_NONE, RAW_VALUE));

        assertThat(result.errors()).containsExactly("Signal data collection '" + RAW_VALUE
                + "' has 0 columns; exactly 3 are required. Adjust the table or column.include.list/column.exclude.list accordingly.");
    }

    @Test
    public void shouldFailWhenEffectiveColumnCountAboveRequired() throws SQLException {
        // More than 3 effective columns are dropped at runtime by parseSignallingMessage, so this must fail like below-3.
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnSizes(resolved)).thenReturn(columnSizes("id", 42, "type", 32, "data", 2048, "created_at", 32, "note", 32));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors()).containsExactly("Signal data collection '" + RAW_VALUE
                + "' has 5 columns; exactly 3 are required. Adjust the table or column.include.list/column.exclude.list accordingly.");
    }

    @Test
    public void shouldWarnButNotFailWhenColumnSizeBelowRecommended() throws SQLException {
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnSizes(resolved)).thenReturn(columnSizes("id", 42, "type", 32, "data", 1000));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors()).isEmpty();
        assertThat(result.warnings()).containsExactly("Signal data collection '" + RAW_VALUE
                + "' column 'data' has size 1000; at least 2000 is recommended to avoid truncating signal payloads.");
    }

    @Test
    public void shouldSkipSizeCheckWhenColumnSizeUnknown() throws SQLException {
        // LOB columns report no usable size (0), so they must not be flagged as undersized.
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnSizes(resolved)).thenReturn(columnSizes("id", 42, "type", 32, "data", 0));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.isValid()).isTrue();
        assertThat(result.warnings()).isEmpty();
    }

    @Test
    public void shouldWarnButNotFailWhenColumnNamesDeviateFromConvention() throws SQLException {
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnSizes(resolved)).thenReturn(columnSizes("sig_id", 42, "sig_type", 32, "sig_data", 2048));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.isValid()).isTrue();
        assertThat(result.warnings()).containsExactly(
                "Signal data collection '" + RAW_VALUE + "' column 1 is named 'sig_id'; 'id' is recommended.",
                "Signal data collection '" + RAW_VALUE + "' column 2 is named 'sig_type'; 'type' is recommended.",
                "Signal data collection '" + RAW_VALUE + "' column 3 is named 'sig_data'; 'data' is recommended.");
    }

    @Test
    public void shouldFailWhenFirstColumnNotNamedIdUnderInsertDeleteWatermarking() throws SQLException {
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnSizes(resolved)).thenReturn(columnSizes("identifier", 42, "type", 32, "data", 2048));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                request(true, true, true, true, ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors()).containsExactly("Signal data collection '" + RAW_VALUE
                + "' first column must be named 'id' for the insert_delete watermarking strategy, but is 'identifier'.");
    }

    @Test
    public void shouldValidateEveryConfiguredSignalDataCollection() throws SQLException {
        // Each configured signal.data.collection is checked independently; a problem in one must not skip the other.
        String secondRawValue = "dbo.other_signal";
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnSizes(resolved)).thenReturn(wellSizedColumns());
        when(connection.readTableNames(null, "dbo", "other_signal", null)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE, secondRawValue));

        assertThat(result.errors()).containsExactly("Signal data collection '" + secondRawValue + "' does not exist.");
    }

    @Test
    public void shouldSwallowSqlExceptionAndContinueToNextValue() throws SQLException {
        String secondRawValue = "dbo.other_signal";
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenThrow(new SQLException("connection reset"));
        when(connection.readTableNames(null, "dbo", "other_signal", null)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE, secondRawValue));

        assertThat(logInterceptor.containsWarnMessage("Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
        assertThat(result.errors()).containsExactly("Signal data collection '" + secondRawValue + "' does not exist.");
    }

    @Test
    public void shouldSwallowRuntimeExceptionAndNeverThrow() throws SQLException {
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenThrow(new RuntimeException("unexpected"));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(logInterceptor.containsWarnMessage("Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
        assertThat(result.isValid()).isTrue();
    }
}

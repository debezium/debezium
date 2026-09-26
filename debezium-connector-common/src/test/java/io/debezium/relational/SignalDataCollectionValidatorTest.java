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
import java.util.List;
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
 * Unit tests for {@link SignalDataCollectionValidator}: the enablement/gating checks, the three ordered checks
 * (existence, accepted FQN shape, effective column count), and the exception-swallowing guarantee.
 */
@ExtendWith(MockitoExtension.class)
public class SignalDataCollectionValidatorTest {

    private static final String RAW_VALUE = "dbo.debezium_signal";
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";
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
                                                          Predicate<TableId> isSignalDataCollection, ColumnNameFilter columnFilter, String... rawValues) {
        return new SignalDataCollectionValidationRequest(Arrays.asList(rawValues), validationEnabled, streamingCapable, sourceChannelEnabled,
                rawValue -> connection, isSignalDataCollection, columnFilter);
    }

    private SignalDataCollectionValidationRequest enabledRequest(Predicate<TableId> isSignalDataCollection, ColumnNameFilter columnFilter, String... rawValues) {
        return request(true, true, true, isSignalDataCollection, columnFilter, rawValues);
    }

    @Test
    public void shouldDoNothingWhenValidationDisabled() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                request(false, true, true, ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        verifyNoInteractions(connection);
        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldDoNothingWhenNotStreamingCapable() {
        // initial_only never transitions to streaming, so the source channel never reads signal.data.collection -
        // validating it would only produce a misleading failure about a config that's never actually used.
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                request(true, false, true, ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        verifyNoInteractions(connection);
        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldDoNothingWhenSourceChannelDisabled() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                request(true, true, false, ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

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
    public void shouldLogInfoAndNotFailWhenSignalDataCollectionIsValid() throws SQLException {
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.isValid()).isTrue();
        assertThat(result.warnings()).isEmpty();
        assertThat(logInterceptor.containsMessage(LOG_PREFIX + " Signal data collection '" + RAW_VALUE + "' is valid.")).isTrue();
    }

    @Test
    public void shouldResolveCorrectCandidateAmongMultipleMatches() throws SQLException {
        // A 2-part FQN can match same-named tables in more than one catalog (e.g. SqlServer multi-db mode); the
        // one actually configured as the connector's signal data collection must be picked over the rest,
        // regardless of Set iteration order, and only that one's columns are inspected.
        TableId otherCatalogMatch = new TableId("otherDb", "dbo", "debezium_signal");
        TableId configuredMatch = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(otherCatalogMatch, configuredMatch));
        when(connection.getColumnNames(configuredMatch)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(configuredMatch::equals, MATCH_ALL, RAW_VALUE));

        assertThat(result.isValid()).isTrue();
        verify(connection, never()).getColumnNames(otherCatalogMatch);
    }

    @Test
    public void shouldFailWhenTableDoesNotExist() throws SQLException {
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors()).containsExactly("Signal data collection '" + RAW_VALUE + "' does not exist.");
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldFailWhenWrongShapeMatchesOneCandidate() throws SQLException {
        TableId found = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(found));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(table -> false, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors()).containsExactly("signal.data.collection must be '" + found + "', not '" + RAW_VALUE + "'.");
        verify(connection, never()).getColumnNames(any());
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
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldFailWhenColumnFilterReducesSignalTableBelowRequiredColumns() throws SQLException {
        // The most common real-world trigger: a column.include.list scoped to other tables matches none of the
        // signal table's columns (a typo that omits just one of id/type/data has the same effect).
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_NONE, RAW_VALUE));

        assertThat(result.errors()).containsExactly("Signal data collection '" + RAW_VALUE
                + "' has 0 columns; exactly 3 are required. Adjust the table or column.include.list/column.exclude.list accordingly.");
    }

    @Test
    public void shouldWarnButNotFailWhenEffectiveColumnCountAboveRequired() throws SQLException {
        // Extra metadata columns beyond id/type/data are tolerated - only flagged as a warning, since customers
        // may legitimately keep them on the signal table.
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data", "created_at", "note"));

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(
                enabledRequest(ALWAYS_SIGNAL_DATA_COLLECTION, MATCH_ALL, RAW_VALUE));

        assertThat(result.errors()).isEmpty();
        assertThat(result.warnings()).containsExactly("Signal data collection '" + RAW_VALUE
                + "' has 5 columns; exactly 3 are required. Adjust the table or column.include.list/column.exclude.list accordingly.");
    }

    @Test
    public void shouldValidateEveryConfiguredSignalDataCollection() throws SQLException {
        // Multi-task deployments can configure more than one signal.data.collection - each must be checked
        // independently, and a problem in one must not prevent the other from being validated.
        String secondRawValue = "dbo.other_signal";
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));
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

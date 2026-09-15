/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.relational.Tables.ColumnNameFilter;

/**
 * Unit tests for {@link SignalDataCollectionValidator}: the enablement/gating checks, the three ordered checks
 * (existence, accepted FQN shape, effective column count), and the exception-swallowing guarantee. {@link JdbcConnection}
 * and {@link RelationalDatabaseConnectorConfig} are mocked so no live database or concrete connector config is required.
 */
@ExtendWith(MockitoExtension.class)
public class SignalDataCollectionValidatorTest {

    private static final String RAW_VALUE = "dbo.debezium_signal";
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";
    private static final ColumnNameFilter MATCH_ALL = (catalog, schema, table, column) -> true;
    private static final ColumnNameFilter MATCH_NONE = (catalog, schema, table, column) -> false;

    @Mock
    private JdbcConnection connection;

    @Mock
    private RelationalDatabaseConnectorConfig connectorConfig;

    private ConfigValue signalDataCollectionValue;
    private LogInterceptor logInterceptor;

    @BeforeEach
    public void beforeEach() {
        signalDataCollectionValue = new ConfigValue("signal.data.collection");
        logInterceptor = new LogInterceptor(SignalDataCollectionValidator.class);
    }

    /**
     * Stubs the three enablement gates open and configures the given signal data collection values, so the
     * validator's loop body actually runs. Tests for the gates themselves stub only what each gate reads.
     */
    private void stubEnabled(String... rawValues) {
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(true);
        when(connectorConfig.getSnapshotMode()).thenReturn(() -> "streaming");
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of(SourceSignalChannel.CHANNEL_NAME));
        when(connectorConfig.getSignalingDataCollectionIds()).thenReturn(Arrays.asList(rawValues));
    }

    @Test
    public void shouldDoNothingWhenValidationDisabled() {
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(connection);
        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }

    @Test
    public void shouldDoNothingWhenSnapshotModeIsInitialOnly() {
        // initial_only never transitions to streaming, so the source channel never reads signal.data.collection -
        // validating it would only produce a misleading failure about a config that's never actually used.
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(true);
        when(connectorConfig.getSnapshotMode()).thenReturn(() -> "initial_only");

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(connection);
        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }

    @Test
    public void shouldDoNothingWhenSourceChannelDisabled() {
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(true);
        when(connectorConfig.getSnapshotMode()).thenReturn(() -> "streaming");
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of("kafka"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(connection);
        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }

    @Test
    public void shouldSkipBlankSignalDataCollectionValues() {
        stubEnabled(null, " ");

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        verifyNoInteractions(connection);
        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }

    @Test
    public void shouldLogInfoAndNotFailWhenSignalDataCollectionIsValid() throws SQLException {
        stubEnabled(RAW_VALUE);
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_ALL);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
        assertThat(logInterceptor.containsMessage(LOG_PREFIX + " Signal data collection '" + RAW_VALUE + "' is valid.")).isTrue();
    }

    @Test
    public void shouldResolveCorrectCandidateAmongMultipleMatches() throws SQLException {
        // A 2-part FQN can match same-named tables in more than one catalog (e.g. SqlServer multi-db mode); the
        // one actually configured as the connector's signal data collection must be picked over the rest,
        // regardless of Set iteration order, and only that one's columns are inspected.
        stubEnabled(RAW_VALUE);
        TableId otherCatalogMatch = new TableId("otherDb", "dbo", "debezium_signal");
        TableId configuredMatch = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(otherCatalogMatch, configuredMatch));
        // otherCatalogMatch is left unstubbed - isSignalDataCollection() defaults to false for it, and whether it's
        // even queried depends on Set iteration order, which findFirst() may short-circuit past.
        when(connectorConfig.isSignalDataCollection(configuredMatch)).thenReturn(true);
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_ALL);
        when(connection.getColumnNames(configuredMatch)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
        verify(connection, never()).getColumnNames(otherCatalogMatch);
    }

    @Test
    public void shouldFailWhenTableDoesNotExist() throws SQLException {
        stubEnabled(RAW_VALUE);
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages())
                .containsExactly("Signal data collection '" + RAW_VALUE + "' does not exist.");
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldFailWhenWrongShapeMatchesOneCandidate() throws SQLException {
        stubEnabled(RAW_VALUE);
        TableId found = new TableId("testDB", "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(found));
        when(connectorConfig.isSignalDataCollection(found)).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages())
                .containsExactly("signal.data.collection must be '" + found + "', not '" + RAW_VALUE + "'.");
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldFailWhenWrongShapeMatchesMultipleCandidates() throws SQLException {
        // The message must list every candidate, sorted for determinism, instead of picking one via Set iteration order.
        stubEnabled(RAW_VALUE);
        TableId dbTwoMatch = new TableId("db2", "dbo", "debezium_signal");
        TableId dbOneMatch = new TableId("db1", "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(dbTwoMatch, dbOneMatch));
        when(connectorConfig.isSignalDataCollection(dbOneMatch)).thenReturn(false);
        when(connectorConfig.isSignalDataCollection(dbTwoMatch)).thenReturn(false);

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages())
                .containsExactly("signal.data.collection must be one of [" + dbOneMatch + ", " + dbTwoMatch + "], not '" + RAW_VALUE + "'.");
        verify(connection, never()).getColumnNames(any());
    }

    @Test
    public void shouldFailWhenColumnFilterReducesSignalTableBelowRequiredColumns() throws SQLException {
        // The most common real-world trigger: a column.include.list scoped to other tables matches none of the
        // signal table's columns (a typo that omits just one of id/type/data has the same effect).
        stubEnabled(RAW_VALUE);
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_NONE);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).containsExactly("Signal data collection '" + RAW_VALUE
                + "' has 0 columns; exactly 3 are required. Adjust the table or column.include.list/column.exclude.list accordingly.");
    }

    @Test
    public void shouldWarnButNotFailWhenEffectiveColumnCountAboveRequired() throws SQLException {
        // Extra metadata columns beyond id/type/data are tolerated - only flagged as a warning, since customers
        // may legitimately keep them on the signal table.
        stubEnabled(RAW_VALUE);
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_ALL);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data", "created_at", "note"));

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
        assertThat(logInterceptor.containsWarnMessage("Signal data collection '" + RAW_VALUE
                + "' has 5 columns; exactly 3 are required. Adjust the table or column.include.list/column.exclude.list accordingly.")).isTrue();
    }

    @Test
    public void shouldValidateEveryConfiguredSignalDataCollection() throws SQLException {
        // Multi-task deployments can configure more than one signal.data.collection - each must be checked
        // independently, and a problem in one must not prevent the other from being validated.
        String secondRawValue = "dbo.other_signal";
        stubEnabled(RAW_VALUE, secondRawValue);
        TableId resolved = new TableId(null, "dbo", "debezium_signal");
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenReturn(Set.of(resolved));
        when(connectorConfig.isSignalDataCollection(resolved)).thenReturn(true);
        when(connectorConfig.getColumnFilter()).thenReturn(MATCH_ALL);
        when(connection.getColumnNames(resolved)).thenReturn(List.of("id", "type", "data"));
        when(connection.readTableNames(null, "dbo", "other_signal", null)).thenReturn(Collections.emptySet());

        SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages())
                .containsExactly("Signal data collection '" + secondRawValue + "' does not exist.");
    }

    @Test
    public void shouldSwallowSqlExceptionAndContinueToNextValue() throws SQLException {
        String secondRawValue = "dbo.other_signal";
        stubEnabled(RAW_VALUE, secondRawValue);
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenThrow(new SQLException("connection reset"));
        when(connection.readTableNames(null, "dbo", "other_signal", null)).thenReturn(Collections.emptySet());

        assertThatCode(() -> SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue))
                .doesNotThrowAnyException();

        assertThat(logInterceptor.containsWarnMessage("Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
        assertThat(signalDataCollectionValue.errorMessages())
                .containsExactly("Signal data collection '" + secondRawValue + "' does not exist.");
    }

    @Test
    public void shouldSwallowRuntimeExceptionAndNeverThrow() throws SQLException {
        stubEnabled(RAW_VALUE);
        when(connection.readTableNames(null, "dbo", "debezium_signal", null)).thenThrow(new RuntimeException("unexpected"));

        assertThatCode(() -> SignalDataCollectionValidator.validate(connection, connectorConfig, signalDataCollectionValue))
                .doesNotThrowAnyException();

        assertThat(logInterceptor.containsWarnMessage("Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }
}

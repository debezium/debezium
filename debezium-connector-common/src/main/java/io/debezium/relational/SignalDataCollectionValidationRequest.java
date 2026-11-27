/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import io.debezium.config.CommonConnectorConfig.WatermarkStrategy;
import io.debezium.data.Envelope.Operation;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.relational.Tables.ColumnNameFilter;

/**
 * Plain inputs {@link SignalDataCollectionValidator#validate} needs, independent of any specific connector config
 * type so it can be invoked outside a Kafka Connect connector too.
 *
 * @author Debezium Authors
 */
public final class SignalDataCollectionValidationRequest {

    private static final String INITIAL_ONLY_SNAPSHOT_MODE = "initial_only";

    private final List<String> rawValues;
    private final boolean validationEnabled;
    private final boolean streamingCapable;
    private final boolean sourceChannelEnabled;
    private final boolean insertDeleteWatermarking;
    private final Set<Operation> skippedSignalOperations;
    private final Function<String, JdbcConnection> connectionResolver;
    private final Predicate<TableId> isSignalDataCollection;
    private final ColumnNameFilter columnFilter;
    private final ConnectorValidator connectorCheck;

    /**
     * @param rawValues the configured signal.data.collection values
     * @param validationEnabled whether validation should run at all
     * @param streamingCapable whether the connector will actually reach a mode that reads signals
     * @param sourceChannelEnabled whether the source signal channel is enabled
     * @param insertDeleteWatermarking whether the insert_delete watermarking strategy is in use, which requires an 'id' column
     * @param skippedSignalOperations the skipped operations that signals rely on and the connector drops before reading them
     * @param connectionResolver resolves the connection to probe with for a given raw value
     * @param isSignalDataCollection matches a resolved table against the connector's actual signal data collection
     * @param columnFilter the connector's effective column filter
     * @param connectorCheck connector-specific checks of the resolved signal data collection
     */
    public SignalDataCollectionValidationRequest(List<String> rawValues, boolean validationEnabled, boolean streamingCapable,
                                                 boolean sourceChannelEnabled, boolean insertDeleteWatermarking,
                                                 Set<Operation> skippedSignalOperations,
                                                 Function<String, JdbcConnection> connectionResolver,
                                                 Predicate<TableId> isSignalDataCollection, ColumnNameFilter columnFilter,
                                                 ConnectorValidator connectorCheck) {
        this.rawValues = rawValues;
        this.validationEnabled = validationEnabled;
        this.streamingCapable = streamingCapable;
        this.sourceChannelEnabled = sourceChannelEnabled;
        this.insertDeleteWatermarking = insertDeleteWatermarking;
        this.skippedSignalOperations = skippedSignalOperations;
        this.connectionResolver = connectionResolver;
        this.isSignalDataCollection = isSignalDataCollection;
        this.columnFilter = columnFilter;
        this.connectorCheck = connectorCheck;
    }

    /**
     * Builds a request from a relational connector's config, resolving every raw value to the same connection.
     *
     * @param connectorConfig the connector configuration
     * @param connectionResolver resolves the connection to probe with for a given raw value
     * @param connectorCheck connector-specific checks of the resolved signal data collection
     */
    public static SignalDataCollectionValidationRequest forConnector(RelationalDatabaseConnectorConfig connectorConfig,
                                                                     Function<String, JdbcConnection> connectionResolver,
                                                                     ConnectorValidator connectorCheck) {
        return new SignalDataCollectionValidationRequest(connectorConfig.getSignalingDataCollectionIds(),
                connectorConfig.isSignalDataCollectionValidationEnabled(),
                !INITIAL_ONLY_SNAPSHOT_MODE.equals(connectorConfig.getSnapshotMode().getValue()),
                connectorConfig.getEnabledChannels().contains(SourceSignalChannel.CHANNEL_NAME),
                WatermarkStrategy.INSERT_DELETE == connectorConfig.getIncrementalSnapshotWatermarkingStrategy(),
                connectorConfig.getSkippedSignalOperations(), connectionResolver, connectorConfig::isSignalDataCollection,
                connectorConfig.getColumnFilter(), connectorCheck);
    }

    public List<String> rawValues() {
        return rawValues;
    }

    public boolean validationEnabled() {
        return validationEnabled;
    }

    public boolean streamingCapable() {
        return streamingCapable;
    }

    public boolean sourceChannelEnabled() {
        return sourceChannelEnabled;
    }

    public boolean insertDeleteWatermarking() {
        return insertDeleteWatermarking;
    }

    public Set<Operation> skippedSignalOperations() {
        return skippedSignalOperations;
    }

    public Function<String, JdbcConnection> connectionResolver() {
        return connectionResolver;
    }

    public Predicate<TableId> isSignalDataCollection() {
        return isSignalDataCollection;
    }

    public ColumnNameFilter columnFilter() {
        return columnFilter;
    }

    public ConnectorValidator connectorCheck() {
        return connectorCheck;
    }

    /**
     * Connector-specific check of a signal data collection that exists and matches the configured name.
     */
    @FunctionalInterface
    public interface ConnectorValidator {

        ConnectorValidator NONE = (signalDataCollection, errors) -> {
        };

        /**
         * @param signalDataCollection the resolved signal data collection
         * @param errors the list to add problems that prevent signals from being read to
         */
        void check(TableId signalDataCollection, List<String> errors) throws SQLException;
    }
}

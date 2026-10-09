/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.relational;

import java.util.Optional;

import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.history.HistoryRecordComparator;

/**
 * A minimal {@link HistorizedRelationalDatabaseConnectorConfig} for tests that need a schema history.
 */
public class TestHistorizedRelationalDatabaseConfig extends HistorizedRelationalDatabaseConnectorConfig {

    public TestHistorizedRelationalDatabaseConfig(Configuration config) {
        super(SourceConnector.class, config, null, null, false, 0, ColumnFilterMode.SCHEMA, false);
    }

    @Override
    public String getContextName() {
        return "test";
    }

    @Override
    public String getConnectorName() {
        return "test";
    }

    @Override
    public EnumeratedValue getSnapshotMode() {
        return null;
    }

    @Override
    public Optional<EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return null;
    }

    @Override
    public HistoryRecordComparator getHistoryRecordComparator() {
        return HistoryRecordComparator.INSTANCE;
    }
}

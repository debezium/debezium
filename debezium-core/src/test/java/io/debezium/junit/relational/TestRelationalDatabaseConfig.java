/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit.relational;

import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Selectors;
import io.debezium.relational.Tables;

public class TestRelationalDatabaseConfig extends RelationalDatabaseConnectorConfig {

    public TestRelationalDatabaseConfig(Configuration config, String logicalName, Tables.TableFilter systemTablesFilter,
                                        Selectors.TableIdToStringMapper tableIdMapper, int defaultSnapshotFetchSize) {
        super(config, logicalName, systemTablesFilter, tableIdMapper, defaultSnapshotFetchSize, ColumnFilterMode.SCHEMA);
    }

    @Override
    public String getContextName() {
        return null;
    }

    @Override
    public String getConnectorName() {
        return null;
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return null;
    }
}

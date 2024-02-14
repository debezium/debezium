/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.mode;

import java.sql.SQLException;
import java.util.Map;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.snapshot.mode.HistorizedSnapshotter;

public class InitialSnapshotter extends HistorizedSnapshotter {

    @Override
    public String name() {
        return OracleConnectorConfig.SnapshotMode.INITIAL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void validate(boolean offsetContextExists, boolean isSnapshotInProgress) {

        final CommonConnectorConfig config = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);
        final JdbcConnection connection = beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, JdbcConnection.class);
        final Offsets<Partition, OffsetContext> offsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        final OffsetContext offset = offsets.getTheOnlyOffset();

        try {
            if (offset != null && !offset.isSnapshotRunning()) {
                // Check to see if the server still has those binlog coordinates ...
                if (!connection.isLogPositionAvailable(offset, config)) {
                    throw new DebeziumException("The connector is trying to read binlog starting at " + offset + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");

                }
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to get last available log position", e);
        }

    }

    @Override
    protected boolean shouldSnapshotWhenNoOffset() {
        return true;
    }

    @Override
    protected boolean shouldSnapshotSchemaWhenNoOffset() {
        return true;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return false;
    }

    @Override
    public boolean shouldSnapshotOnDataError() { // TODO check with DBZ-7308
        return false;
    }
}

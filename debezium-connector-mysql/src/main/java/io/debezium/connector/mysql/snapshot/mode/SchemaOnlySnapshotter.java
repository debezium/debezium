/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import java.util.Map;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbConnection;
import io.debezium.connector.mysql.strategy.mariadb.MariaDbConnectorAdapter;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnection;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.snapshot.mode.HistorizedSnapshotter;

public class SchemaOnlySnapshotter extends HistorizedSnapshotter {

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void validate(boolean offsetContextExists, boolean isSnapshotInProgress) {

        final MySqlConnectorConfig config = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, MySqlConnectorConfig.class);
        final AbstractConnectorConnection connection = beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, getConnectionClass(config));
        final Offsets<MySqlPartition, MySqlOffsetContext> mySqloffsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        final MySqlOffsetContext offset = mySqloffsets.getTheOnlyOffset();

        if (offset != null && !offset.isSnapshotRunning()) {
            // Check to see if the server still has those binlog coordinates ...
            if (!connection.isBinlogPositionAvailable(config, offset.gtidSet(), offset.getSource().binlogFilename())) {
                throw new DebeziumException("The connector is trying to read binlog starting at " + offset.getSource() + ", but this is no longer "
                        + "available on the server. Reconfigure the connector to use a snapshot when needed.");

            }
        }

    }

    @Override
    protected boolean shouldSnapshotWhenNoOffset() {
        return false;
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
    public boolean shouldSnapshotOnDataError() {
        return false;
    }

    private Class<? extends AbstractConnectorConnection> getConnectionClass(MySqlConnectorConfig config) {
        // TODO review this when MariaDB becomes a first class connector
        if (config.getConnectorAdapter() instanceof MariaDbConnectorAdapter) {
            return MariaDbConnection.class;
        }
        else {
            return MySqlConnection.class;
        }
    }
}

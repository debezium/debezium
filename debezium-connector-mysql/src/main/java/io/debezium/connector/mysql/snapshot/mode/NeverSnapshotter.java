/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnection;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.spi.snapshot.Snapshotter;

public class NeverSnapshotter extends BeanAwareSnapshotter implements Snapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NeverSnapshotter.class);

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotMode.NEVER.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void validate(boolean offsetContextExists, boolean isSnapshotInProgress) {

        final MySqlConnection connection = beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, MySqlConnection.class);
        final MySqlConnectorConfig config = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, MySqlConnectorConfig.class);
        final Offsets<MySqlPartition, MySqlOffsetContext> mySqloffsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        final MySqlOffsetContext offset = mySqloffsets.getTheOnlyOffset();

        if (offsetContextExists) {
            if (isSnapshotInProgress) {
                // The last offset was an incomplete snapshot and now the snapshot was disabled
                throw new DebeziumException("The connector previously stopped while taking a snapshot, but now the connector is configured "
                        + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.");
            }
            else {
                // But check to see if the server still has those binlog coordinates ...
                if (!connection.isBinlogPositionAvailable(config, offset.gtidSet(), offset.getSource().binlogFilename())) {
                    throw new DebeziumException("The connector is trying to read binlog starting at " + offset.getSource() + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");

                }
            }
        }

        // Look to see what the first available binlog file is called, and whether it looks like binlog files have
        // been purged. If so, then output a warning ...
        String earliestBinlogFilename = connection.earliestBinlogFilename();
        if (earliestBinlogFilename == null) {
            LOGGER.warn("No binlog appears to be available. Ensure that the MySQL row-level binlog is enabled.");
        }
        else if (!earliestBinlogFilename.endsWith("00001")) {
            LOGGER.warn("It is possible the server has purged some binlogs. If this is the case, then using snapshot mode may be required.");
        }
    }

    @Override
    public boolean shouldSnapshot() {
        return false;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshotSchema() {
        return false;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return false;
    }

    @Override
    public boolean shouldSnapshotOnDataError() {
        return false;
    }
}

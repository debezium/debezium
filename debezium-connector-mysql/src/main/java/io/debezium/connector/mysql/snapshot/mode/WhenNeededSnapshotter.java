/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.snapshot.mode;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnection;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.spi.snapshot.Snapshotter;

public class WhenNeededSnapshotter implements Snapshotter, BeanRegistryAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(WhenNeededSnapshotter.class);

    private BeanRegistry beanRegistry;

    @Override
    public String name() {
        return MySqlConnectorConfig.SnapshotMode.WHEN_NEEDED.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override
    public void validate(boolean offsetContextExists, boolean isSnapshotInProgress) {

        final MySqlConnection connection = beanRegistry.lookupByName(StandardBeanNames.JDBC_CONNECTION, MySqlConnection.class);
        final MySqlConnectorConfig config = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, MySqlConnectorConfig.class);
        final Offsets<MySqlPartition, MySqlOffsetContext> mySqloffsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        final MySqlOffsetContext offset = mySqloffsets.getTheOnlyOffset();

        if (offset != null && !offset.isSnapshotRunning()) {
            // Check to see if the server still has those binlog coordinates ...
            if (!connection.isBinlogPositionAvailable(config, offset.gtidSet(), offset.getSource().binlogFilename())) {
                LOGGER.warn(
                        "The connector is trying to read binlog starting at '{}', but this is no longer available on the server. Forcing the snapshot execution as it is allowed by the configuration.",
                        offset.getSource());
                mySqloffsets.resetOffset(mySqloffsets.getTheOnlyPartition());

            }
        }
    }

    @Override
    public boolean shouldSnapshot() {
        return true;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshotSchema() {
        return true;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return false;
    }

    @Override
    public boolean shouldSnapshotOnDataError() {
        return true;
    }
}

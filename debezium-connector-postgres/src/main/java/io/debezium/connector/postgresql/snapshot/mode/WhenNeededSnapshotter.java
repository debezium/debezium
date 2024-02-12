/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot.mode;

import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.snapshot.mode.BeanAwareSnapshotter;
import io.debezium.spi.snapshot.Snapshotter;

public class WhenNeededSnapshotter extends BeanAwareSnapshotter implements Snapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(WhenNeededSnapshotter.class);

    @Override
    public String name() {
        return PostgresConnectorConfig.SnapshotMode.WHEN_NEEDED.getValue();
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
            if (offsetContextExists && !isSnapshotInProgress) {
                // Check to see if the server still has those log coordinates ...
                if (!connection.isLogPositionAvailable(offset, config)) {
                    LOGGER.warn(
                            "The connector is trying to read log starting at '{}', but this is no longer available on the server. Forcing the snapshot execution as it is allowed by the configuration.",
                            getStoredLogPosition(offset));
                    offsets.resetOffset(offsets.getTheOnlyPartition()); // TODO DBZ-7308 evaluate if side effect can be extracted
                }
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to get last available log position", e);
        }
    }

    private Object getStoredLogPosition(OffsetContext offset) {
        return ((PostgresOffsetContext) offset).lastCommitLsn();
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

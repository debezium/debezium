/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.snapshot.mode;

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.spi.snapshot.Snapshotter;

public class NeverSnapshotter implements Snapshotter {

    private final static Logger LOGGER = LoggerFactory.getLogger(NeverSnapshotter.class);

    @Override
    public String name() {
        return PostgresConnectorConfig.SnapshotMode.NEVER.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void validate(boolean offsetContextExists, boolean isSnapshotInProgress) {

        if (offsetContextExists && isSnapshotInProgress) {
            String msg = "The connector previously stopped while taking a snapshot, but now the connector is configured "
                    + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.";
            LOGGER.error(msg);
            throw new ConnectException(msg);
        }
        else {
            LOGGER.info("Snapshots are not allowed as per configuration, starting streaming logical changes only");
        }
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshot() {
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

    @Override
    public boolean shouldSnapshotSchema() {
        return false;
    }
}

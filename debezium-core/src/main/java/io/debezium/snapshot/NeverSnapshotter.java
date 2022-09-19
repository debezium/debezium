/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.snapshot.Snapshotter;

@Incubating
public class NeverSnapshotter implements Snapshotter<OffsetContext> {

    private final static Logger LOGGER = LoggerFactory.getLogger(NeverSnapshotter.class);

    @Override
    public void configure(Properties properties, OffsetContext offsetContext) {
        if (offsetContext != null && offsetContext.isSnapshotRunning()) {
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
    public boolean shouldSnapshot() {
        return false;
    }

    @Override
    public boolean includeSchema() {
        return false;
    }

    @Override
    public boolean includeData() {
        return false;
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

    @Override
    public Optional<String> buildSnapshotQuery(DataCollectionId id, List<String> snapshotSelectColumns, char quotingChar) {
        throw new UnsupportedOperationException("'never' snapshot mode cannot build queries");
    }
}

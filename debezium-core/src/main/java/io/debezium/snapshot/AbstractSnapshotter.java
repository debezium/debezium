/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.snapshot.Snapshotter;

@Incubating
public abstract class AbstractSnapshotter implements Snapshotter<OffsetContext> {
    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractSnapshotter.class);

    protected OffsetContext offsetContext;
    protected Configuration config;

    @Override
    public void configure(Properties props, OffsetContext offsetContext) {
        this.offsetContext = offsetContext;
        this.config = Configuration.from(props);
    }

    @Override
    public boolean shouldSnapshot() {
        if (offsetContext == null) {
            LOGGER.info("Taking initial snapshot for new datasource");
            return true;
        }
        else if (offsetContext.isSnapshotRunning()) {
            LOGGER.info("Found previous incomplete snapshot");
            return true;
        }
        else {
            LOGGER.info("Previous initial snapshot completed, no snapshot will be performed");
            return false;
        }
    }

    @Override
    public boolean includeSchema() {
        return shouldSnapshot();
    }

    @Override
    public boolean includeData() {
        return shouldSnapshot();
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
        if (id instanceof TableId) {
            String query = snapshotSelectColumns.stream()
                    .collect(Collectors.joining(", ", "SELECT ", " FROM " + ((TableId) id).toQuotedString(quotingChar)));
            return Optional.of(query);
        }
        return Optional.empty();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.transforms.timescaledb;

import java.io.IOException;
import java.util.Optional;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;

public class TestMetadata extends AbstractTimescaleDbMetadata {

    protected TestMetadata(Configuration config) {
        super(config);
    }

    @Override
    public Optional<TableId> hypertableId(TableId chunkId) {
        switch (chunkId.table()) {
            case "_hyper_1_1_chunk":
                return Optional.of(new TableId(null, "public", "conditions"));
            case "_hyper_2_2_chunk":
                return Optional.of(new TableId(null, chunkId.schema(), "_materialized_hypertable_2"));
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableId> aggregateId(TableId hypertableId) {
        switch (hypertableId.table()) {
            case "_materialized_hypertable_2":
                return Optional.of(new TableId(null, "public", "conditions_summary"));
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
    }
}

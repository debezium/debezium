/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.snapshot.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.snapshot.spi.SnapshotQuery;

@ConnectorSpecific(connector = MongoDbConnector.class)
public class SelectAllSnapshotQuery implements SnapshotQuery {

    @Override
    public String name() {
        return CommonConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {

        return Optional.empty(); // TODO check if need to implement
    }
}

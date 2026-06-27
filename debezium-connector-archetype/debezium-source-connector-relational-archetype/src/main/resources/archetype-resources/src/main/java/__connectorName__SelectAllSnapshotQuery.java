/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package ${package};

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.snapshot.spi.SnapshotQuery;

/**
 * The default snapshot SELECT for ${connectorName}: reads the requested columns from a table.
 *
 * <p>Registered as a {@link SnapshotQuery} SPI service in
 * {@code META-INF/services/io.debezium.snapshot.spi.SnapshotQuery}. The snapshotter selects it by
 * the {@code snapshot.query.mode} value ({@code select_all}, the default). Override
 * {@link #snapshotQuery} if your database needs a different SELECT.
 */
@ConnectorSpecific(connector = ${connectorName}SourceConnector.class)
public class ${connectorName}SelectAllSnapshotQuery implements SnapshotQuery {

    @Override
    public String name() {
        return CommonConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {
    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {
        String columns = String.join(", ", snapshotSelectColumns);
        return Optional.of(String.format("SELECT %s FROM %s", columns, tableId));
    }
}

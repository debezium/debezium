/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.snapshot.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.bean.spi.BeanRegistryAware;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.snapshot.spi.SnapshotQuery;

public class SelectAllSnapshotQuery implements SnapshotQuery, BeanRegistryAware {

    private BeanRegistry beanRegistry;

    @Override
    public String name() {
        return CommonConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public void injectBeanRegistry(BeanRegistry beanRegistry) {
        this.beanRegistry = beanRegistry;
    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {

        final Offsets<OraclePartition, OracleOffsetContext> mySqloffsets = beanRegistry.lookupByName(StandardBeanNames.OFFSETS, Offsets.class);
        final OracleOffsetContext offset = mySqloffsets.getTheOnlyOffset();

        final String snapshotOffset = offset.getScn().toString();
        String columns = String.join(", ", snapshotSelectColumns);
        assert snapshotOffset != null;
        return Optional.of(String.format("SELECT %s FROM %s AS OF SCN %s", columns, tableId, snapshotOffset));

    }
}

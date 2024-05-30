/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

import java.util.Map;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.spi.snapshot.Snapshotter;

public class ConfigurationBasedSnapshotter extends BeanAwareSnapshotter implements Snapshotter {
    private boolean snapshotData;
    private boolean snapshotSchema;
    private boolean stream;
    private boolean snapshotOnSchemaError;
    private boolean snapshotOnDataError;

    @Override
    public String name() {
        return "configuration_based";
    }

    @Override
    public void configure(Map<String, ?> properties) {

        CommonConnectorConfig commonConnectorConfig = beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, CommonConnectorConfig.class);
        this.snapshotData = commonConnectorConfig.snapshotModeConfigurationBasedSnapshotData();
        this.snapshotSchema = commonConnectorConfig.snapshotModeConfigurationBasedSnapshotSchema();
        this.stream = commonConnectorConfig.snapshotModeConfigurationBasedStream();
        this.snapshotOnSchemaError = commonConnectorConfig.snapshotModeConfigurationBasedSnapshotOnSchemaError();
        this.snapshotOnDataError = commonConnectorConfig.snapshotModeConfigurationBasedSnapshotOnDataError();
    }

    @Override
    public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
        return this.snapshotData;
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {
        return this.snapshotSchema;
    }

    @Override
    public boolean shouldStream() {
        return this.stream;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return this.snapshotOnSchemaError;
    }

    @Override
    public boolean shouldSnapshotOnDataError() {
        return this.snapshotOnDataError;
    }
}

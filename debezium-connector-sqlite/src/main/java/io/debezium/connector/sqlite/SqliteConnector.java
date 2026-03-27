/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Kafka Connect source connector for SQLite databases.
 * <p>
 * This connector monitors a SQLite database by reading its WAL (Write-Ahead Logging) file
 * to detect committed changes, decodes b-tree page images to reconstruct row-level events,
 * and emits standard Debezium change events.
 *
 * @author Zihan Dai
 */
public class SqliteConnector extends RelationalBaseSourceConnector {

    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SqliteConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // SQLite is a single-file database; only one task is supported.
        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return SqliteConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        // TODO: Validate that the database file exists and is readable,
        // and that WAL mode is enabled.
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(SqliteConnectorConfig.ALL_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends DataCollectionId> List<T> getMatchingCollections(Configuration config) {
        // TODO: Read table list from SQLite's sqlite_master and filter by table.include.list.
        return Collections.emptyList();
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.datastax.driver.core.TableMetadata;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorSchemaException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Caches the key and value schema for all CDC-enabled tables. This cache gets updated
 * by {@link SchemaProcessor} periodically.
 */
public class SchemaHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHolder.class);

    private final Map<KeyspaceTable, KeyValueSchema> tableToKVSchemaMap = new ConcurrentHashMap<>();

    private final CassandraClient cassandraClient;
    private final String connectorName;

    public SchemaHolder(CassandraClient cassandraClient, String connectorName) {
        this.cassandraClient = cassandraClient;
        this.connectorName = connectorName;
        refreshSchemas();
    }

    public void refreshSchemas() {
        LOGGER.debug("Refreshing schemas...");
        Map<KeyspaceTable, TableMetadata> latest = getLatestTableMetadatas();
        removeDeletedTableSchemas(latest);
        createOrUpdateNewTableSchemas(latest);
        LOGGER.debug("Schemas are refreshed");
    }

    public KeyValueSchema getOrUpdateKeyValueSchema(KeyspaceTable kt) {
        if (!tableToKVSchemaMap.containsKey(kt)) {
            refreshSchema(kt);
        }
        return tableToKVSchemaMap.getOrDefault(kt, null);
    }

    public Set<TableMetadata> getCdcEnabledTableMetadataSet() {
        return tableToKVSchemaMap.values().stream()
                .map(KeyValueSchema::tableMetadata)
                .filter(tm -> tm.getOptions().isCDC())
                .collect(Collectors.toSet());
    }

    /**
     * Get the schema of an inner field based on the field name
     * @param fieldName the name of the field in the schema
     * @param schema the schema where the field resides in
     * @return Schema
     */
    public static Schema getFieldSchema(String fieldName, Schema schema) {
        if (schema.getType().equals(Schema.Type.UNION)) {
            List<Schema> unionOfSchemas = schema.getTypes();
            for (Schema innerSchema : unionOfSchemas) {
                if (innerSchema.getName().equals(fieldName)) {
                    return innerSchema;
                }
            }
            throw new CassandraConnectorSchemaException("Union type does not contain field " + fieldName);
        } else if (schema.getType().equals(Schema.Type.RECORD)) {
            return schema.getField(fieldName).schema();
        } else {
            throw new CassandraConnectorSchemaException("Only UNION and RECORD types are supported for this method, but encountered " + schema.getType());
        }
    }

    private void refreshSchema(KeyspaceTable keyspaceTable) {
        LOGGER.debug("Refreshing schema for {}", keyspaceTable);
        TableMetadata existing = tableToKVSchemaMap.containsKey(keyspaceTable) ?  tableToKVSchemaMap.get(keyspaceTable).tableMetadata() : null;
        TableMetadata latest = cassandraClient.getCdcEnabledTableMetadata(keyspaceTable.keyspace, keyspaceTable.table);
        if (existing != latest) {
            if (existing == null) {
                tableToKVSchemaMap.put(keyspaceTable, new KeyValueSchema(connectorName, latest));
                LOGGER.debug("Updated schema for {}", keyspaceTable);
            }
            if (latest == null) {
                tableToKVSchemaMap.remove(keyspaceTable);
                LOGGER.debug("Removed schema for {}", keyspaceTable);
            }
        }
    }

    private Map<KeyspaceTable, TableMetadata> getLatestTableMetadatas() {
        Map<KeyspaceTable, TableMetadata> latest = new HashMap<>();
        for (TableMetadata tm : cassandraClient.getCdcEnabledTableMetadataList()) {
            latest.put(new KeyspaceTable(tm), tm);
        }
        return latest;
    }

    private void removeDeletedTableSchemas(Map<KeyspaceTable, TableMetadata> latestTableMetadataMap) {
        Set<KeyspaceTable> existingTables = tableToKVSchemaMap.keySet();
        Set<KeyspaceTable> latestTables = latestTableMetadataMap.keySet();
        existingTables.removeAll(latestTables);
        tableToKVSchemaMap.keySet().removeAll(existingTables);
    }

    private void createOrUpdateNewTableSchemas(Map<KeyspaceTable, TableMetadata> latestTableMetadataMap) {
        latestTableMetadataMap.forEach((table, metadata) -> {
            TableMetadata existingTableMetadata = tableToKVSchemaMap.containsKey(table) ? tableToKVSchemaMap.get(table).tableMetadata() : null;
            if (existingTableMetadata != metadata) {
                KeyValueSchema keyValueSchema = new KeyValueSchema(connectorName, metadata);
                tableToKVSchemaMap.put(table, keyValueSchema);
                LOGGER.debug("Updated schema for {}", table);
            }
        });
    }

    public static class KeyValueSchema {
        private final TableMetadata tableMetadata;
        private final Schema keySchema;
        private final Schema valueSchema;

        KeyValueSchema(String connectorName, TableMetadata tableMetadata) {
            this.tableMetadata = tableMetadata;
            this.keySchema = Record.keySchema(connectorName, tableMetadata);
            this.valueSchema = Record.valueSchema(connectorName, tableMetadata);
        }

        public TableMetadata tableMetadata() {
            return tableMetadata;
        }

        public Schema keySchema() {
            return keySchema;
        }

        public Schema valueSchema() {
            return valueSchema;
        }
    }
}

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;

import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorSchemaException;
import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

/**
 * Caches the key and value schema for all CDC-enabled tables. This cache gets updated
 * by {@link SchemaProcessor} periodically.
 */
public class SchemaHolder {

    private static final String NAMESPACE = "io.debezium.connector.cassandra";
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaHolder.class);

    private final Map<KeyspaceTable, KeyValueSchema> tableToKVSchemaMap = new ConcurrentHashMap<>();

    private final CassandraClient cassandraClient;
    private final String connectorName;
    private final SourceInfoStructMaker sourceInfoStructMaker;

    public SchemaHolder(CassandraClient cassandraClient, String connectorName, SourceInfoStructMaker sourceInfoStructMaker) {
        this.cassandraClient = cassandraClient;
        this.connectorName = connectorName;
        this.sourceInfoStructMaker = sourceInfoStructMaker;
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
        if (schema.type().equals(Schema.Type.STRUCT)) {
            return schema.field(fieldName).schema();
        }
        throw new CassandraConnectorSchemaException("Only STRUCT type is supported for this method, but encountered " + schema.type());
    }

    private void refreshSchema(KeyspaceTable keyspaceTable) {
        LOGGER.debug("Refreshing schema for {}", keyspaceTable);
        TableMetadata existing = tableToKVSchemaMap.containsKey(keyspaceTable) ? tableToKVSchemaMap.get(keyspaceTable).tableMetadata() : null;
        TableMetadata latest = cassandraClient.getCdcEnabledTableMetadata(keyspaceTable.keyspace, keyspaceTable.table);
        if (existing != latest) {
            if (existing == null) {
                tableToKVSchemaMap.put(keyspaceTable, new KeyValueSchema(connectorName, latest, sourceInfoStructMaker));
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
                KeyValueSchema keyValueSchema = new KeyValueSchema(connectorName, metadata, sourceInfoStructMaker);
                tableToKVSchemaMap.put(table, keyValueSchema);
                LOGGER.debug("Updated schema for {}", table);
            }
        });
    }

    public static class KeyValueSchema {
        private final TableMetadata tableMetadata;
        private final Schema keySchema;
        private final Schema valueSchema;

        KeyValueSchema(String connectorName, TableMetadata tableMetadata, SourceInfoStructMaker sourceInfoStructMaker) {
            this.tableMetadata = tableMetadata;
            this.keySchema = getKeySchema(connectorName, tableMetadata);
            this.valueSchema = getValueSchema(connectorName, tableMetadata, sourceInfoStructMaker);
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

        private Schema getKeySchema(String connectorName, TableMetadata tm) {
            if (tm == null) {
                return null;
            }
            SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(NAMESPACE + "." + getKeyName(connectorName, tm));
            for (ColumnMetadata cm : tm.getPrimaryKey()) {
                AbstractType<?> convertedType = CassandraTypeConverter.convert(cm.getType());
                Schema colSchema = CassandraTypeDeserializer.getSchemaBuilder(convertedType).build();
                if (colSchema != null) {
                    schemaBuilder.field(cm.getName(), colSchema);
                }
            }
            return schemaBuilder.build();
        }

        private Schema getValueSchema(String connectorName, TableMetadata tm, SourceInfoStructMaker sourceInfoStructMaker) {
            if (tm == null) {
                return null;
            }
            return SchemaBuilder.struct().name(NAMESPACE + "." + getValueName(connectorName, tm))
                    .field(Record.TIMESTAMP, Schema.INT64_SCHEMA)
                    .field(Record.OPERATION, Schema.STRING_SCHEMA)
                    .field(Record.SOURCE, sourceInfoStructMaker.schema())
                    .field(Record.AFTER, RowData.rowSchema(tm))
                    .build();
        }

        private static String getKeyName(String connectorName, TableMetadata tm) {
            return connectorName + "." + tm.getKeyspace().getName() + "." + tm.getName() + ".Key";
        }

        private static String getValueName(String connectorName, TableMetadata tm) {
            return connectorName + "." + tm.getKeyspace().getName() + "." + tm.getName() + ".Value";
        }
    }
}

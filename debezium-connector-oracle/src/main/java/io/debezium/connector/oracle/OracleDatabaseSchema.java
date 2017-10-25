/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.Tables;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

// TODO generify into HistorizedRelationalDatabaseSchema

public class OracleDatabaseSchema implements RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseSchema.class);

    private final TopicSelector topicSelector;

    private final Tables tables;
    private final Map<TableId, TableSchema> schemas;
    private final TableSchemaBuilder tableSchemaBuilder;

    public OracleDatabaseSchema(SchemaNameAdjuster schemaNameAdjuster, TopicSelector topicSelector) {
        this.topicSelector = topicSelector;

        this.tables = new Tables();
        this.schemas = new HashMap<>();
        this.tableSchemaBuilder = new TableSchemaBuilder(new OracleValueConverters(), schemaNameAdjuster, SourceInfo.SCHEMA);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        LOGGER.debug("Applying schema change event {}", schemaChange);

        Table table = schemaChange.getTable();

        tables.overwriteTable(table);
        schemas.put(table.id(), tableSchemaBuilder.create(null, getEnvelopeSchemaName(table), table, null, null));
    }

    private String getEnvelopeSchemaName(Table table) {
        return topicSelector.topicNameFor(table.id()) + ".Envelope";
    }

    @Override
    public DataCollectionSchema getDataCollectionSchema(DataCollectionId id) {
        return schemas.get(id);
    }

    @Override
    public Table getTable(TableId id) {
        return tables.forTable(id);
    }
}

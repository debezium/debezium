/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryMetrics;

/**
 * Configuration options shared across the relational CDC connectors which use a persistent database schema history.
 *
 * @author Gunnar Morling
 */
public abstract class HistorizedRelationalDatabaseConnectorConfig extends RelationalDatabaseConnectorConfig {

    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;

    private static final String DEFAULT_SCHEMA_HISTORY = "io.debezium.storage.kafka.history.KafkaSchemaHistory";

    private boolean useCatalogBeforeSchema;
    private final String logicalName;
    private final Class<? extends SourceConnector> connectorClass;
    private final boolean multiPartitionMode;

    /**
     * The database history class is hidden in the {@link #configDef()} since that is designed to work with a user interface,
     * and in these situations using Kafka is the only way to go.
     */
    public static final Field SCHEMA_HISTORY = Field.create("schema.history")
            .withDisplayName("Database history class")
            .withType(Type.CLASS)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withInvisibleRecommender()
            .withDescription("The name of the SchemaHistory class that should be used to store and recover database schema changes. "
                    + "The configuration properties for the history are prefixed with the '"
                    + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "' string.")
            .withDefault(DEFAULT_SCHEMA_HISTORY);

    protected static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .history(
                    SCHEMA_HISTORY,
                    SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS,
                    SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL)
            .create();

    protected HistorizedRelationalDatabaseConnectorConfig(Class<? extends SourceConnector> connectorClass,
                                                          Configuration config, String logicalName,
                                                          TableFilter systemTablesFilter,
                                                          boolean useCatalogBeforeSchema,
                                                          int defaultSnapshotFetchSize,
                                                          ColumnFilterMode columnFilterMode,
                                                          boolean multiPartitionMode) {
        super(config, logicalName, systemTablesFilter, TableId::toString, defaultSnapshotFetchSize, columnFilterMode, useCatalogBeforeSchema);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.logicalName = logicalName;
        this.connectorClass = connectorClass;
        this.multiPartitionMode = multiPartitionMode;
    }

    protected HistorizedRelationalDatabaseConnectorConfig(Class<? extends SourceConnector> connectorClass, Configuration config, String logicalName,
                                                          TableFilter systemTablesFilter, TableIdToStringMapper tableIdMapper,
                                                          boolean useCatalogBeforeSchema, ColumnFilterMode columnFilterMode,
                                                          boolean multiPartitionMode) {
        super(config, logicalName, systemTablesFilter, tableIdMapper, DEFAULT_SNAPSHOT_FETCH_SIZE, columnFilterMode, useCatalogBeforeSchema);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.logicalName = logicalName;
        this.connectorClass = connectorClass;
        this.multiPartitionMode = multiPartitionMode;
    }

    /**
     * Returns a configured (but not yet started) instance of the database history.
     */
    public SchemaHistory getSchemaHistory() {
        Configuration config = getConfig();

        SchemaHistory schemaHistory = config.getInstance(HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY, SchemaHistory.class);
        if (schemaHistory == null) {
            throw new ConnectException("Unable to instantiate the database history class " +
                    config.getString(HistorizedRelationalDatabaseConnectorConfig.SCHEMA_HISTORY));
        }

        // Do not remove the prefix from the subset of config properties ...
        Configuration dbHistoryConfig = config.subset(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(SchemaHistory.NAME, getLogicalName() + "-dbhistory")
                .withDefault(AbstractSchemaHistory.INTERNAL_CONNECTOR_CLASS, connectorClass.getName())
                .withDefault(AbstractSchemaHistory.INTERNAL_CONNECTOR_ID, logicalName)
                .build();

        HistoryRecordComparator historyComparator = getHistoryRecordComparator();
        schemaHistory.configure(dbHistoryConfig, historyComparator,
                new SchemaHistoryMetrics(this, multiPartitionMode()), useCatalogBeforeSchema()); // validates

        return schemaHistory;
    }

    public boolean useCatalogBeforeSchema() {
        return useCatalogBeforeSchema;
    }

    public boolean multiPartitionMode() {
        return multiPartitionMode;
    }

    /**
     * Returns a comparator to be used when recovering records from the schema history, making sure no history entries
     * newer than the offset we resume from are recovered (which could happen when restarting a connector after history
     * records have been persisted but no new offset has been committed yet).
     */
    protected abstract HistoryRecordComparator getHistoryRecordComparator();

}
